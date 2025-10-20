import asyncio
import logging
import traceback
from collections.abc import Coroutine
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from types import NoneType, TracebackType
from typing import Any, Callable, Protocol, Self, get_origin, get_type_hints

from icij_common.logging_utils import DifferedLoggingMessage, LogWithNameMixin
from icij_common.pydantic_utils import safe_copy
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool
from pydantic import TypeAdapter

from kestra_python._types import (
    State,
    StateWithHistory,
    TaskOutputABC,
    TaskRunAttempt,
    WorkerJob,
    WorkerTask,
    WorkerTaskResult,
)
from kestra_python.exceptions import (
    MessageTooBigException,
    QueueException,
    RunnableTaskException,
    UnknownTask,
    UnsupportedMessageException,
)
from kestra_python.postgres_queue import ConsumerType, QueueType, emit, poll_queue
from kestra_python.utils import WithNameLoggingAdapter, do_nothing_cm

_logger = logging.getLogger(__name__)

PYTHON_GROUP = "python"

_DURATION_TYPE_ADAPTER = TypeAdapter(timedelta)

_DEFAULT_MAX_RESULT_SIZE = 104_857_600


class QueuePublisher(Protocol):
    async def __call__(self, msg: Any) -> None: ...


@dataclass(frozen=True)
class ExceptionContext:
    final_state: State | None = None
    exception: Exception | None = None


class Worker(LogWithNameMixin):
    def __init__(
        self,
        task_registry: dict[str, Callable],
        poll: AsyncConnectionPool,
        worker_id: str | None = None,
        *,
        logger: logging.Logger | None = None,
        group: str = PYTHON_GROUP,
        poll_size: int = 1,
        poll_interval_s: float = 0.1,
        result_size_limit: int = _DEFAULT_MAX_RESULT_SIZE,
    ):
        # TODO: handle signals with a __aenter__ and __aexit__
        if logger is None:
            logger = _logger
        self._id = worker_id
        logger = WithNameLoggingAdapter(
            name=self._id, name_label="worker_id", logger=logger
        )
        super().__init__(logger)
        self._task_registry = {
            k: _validate_task_output(v) for k, v in task_registry.items()
        }
        self._pool = poll
        self._killed: Exception | None = None
        self._group = group
        self._poll_size = poll_size
        self._poll_interval_s = poll_interval_s
        self._result_size_limit = result_size_limit
        self._task_conn: AsyncConnection | None = None
        self._task_result_publisher: QueuePublisher | None = None
        self._cancellable: set[asyncio.Task] = set()
        self._exit_stack = AsyncExitStack()
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> Self:
        self._task_conn = await self._exit_stack.enter_async_context(
            self._pool.connection()
        )
        result_conn = await self._exit_stack.enter_async_context(
            self._pool.connection()
        )
        self._task_result_publisher = partial(
            emit,
            conn=result_conn,
            queue_type=QueueType.WORKER_TASK_RESULT,
            size_limit=self._result_size_limit,
        )

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def work(self) -> None:
        self.info("starting working...")
        async with self:
            self.info("starting polling task...")
            while True:
                async for records in poll_queue(
                    self._task_conn,
                    WorkerJob,
                    self._poll_interval_s,
                    consumer_group=self._group,
                    queue_type=QueueType.WORKER_JOB,
                    consumer_type=ConsumerType.WORKER,
                    poll_size=self._poll_size,
                    for_update=True,
                ):
                    for record in records:
                        self.info(
                            "received job: %s", record.model_dump_json(by_alias=True)
                        )
                        if isinstance(record, WorkerTask):
                            await self.handle_task(record)
                        else:
                            raise TypeError(
                                f"unexpected type for worker_job: {type(record)}"
                            )

    async def handle_task(self, task: WorkerTask) -> WorkerTaskResult:
        # TODO: make this function shorter
        # TODO: handle killed tasks here
        self.info(
            "Type %s started: %s",
            task.task.__class__.__name__,
            DifferedLoggingMessage(lambda: task.model_dump_json(by_alias=True)),
        )
        # TODO: handle caching here
        try:
            task_fn = self._task_registry.get(task.task.name)
            if task_fn is None:
                raise UnknownTask(task)
            task_inputs = task.run_context.render(
                task.task.inputs, task.run_context.variables
            )
            task = task.with_state(State.RUNNING)
            task_fn = partial(task_fn, **task_inputs)
            task = await self._run_attempt(task, task_fn)
            dynamic_task_runs = [
                safe_copy(wr.task_run, update={"dynamic": True})
                for wr in task.run_context.dynamic_worker_results
            ]
            task = task.resolve_final_state()
            worker_task_result = WorkerTaskResult(
                task_run=task.task_run, dynamic_task_runs=dynamic_task_runs
            )
            await self._task_result_publisher(worker_task_result)
            return worker_task_result
            # TODO: handle cache saving here
        except (QueueException, UnknownTask) as e:
            failed = task.fail()
            if isinstance(e, (MessageTooBigException, UnsupportedMessageException)):
                failed = failed.with_outputs(dict())
            self.exception("Unable to emit the worker task result to the queue: %s", e)
            worker_task_result = WorkerTaskResult(task_run=failed.task_run)
            try:
                await self._task_result_publisher(worker_task_result)
            except QueueException:
                self.exception(
                    "Unable to emit the worker task result for task {} taskrun {}",
                    task.task.id,
                    task.task_run.id,
                )
            return worker_task_result
        finally:
            self.info(
                "Type %s with state %s completed in %s",
                task.task.__class__.__name__,
                task.task_run.state,
                task.task_run.state.duration,
            )
            # TODO: handle cleanup as needed

    async def _run_attempt(
        self, task: WorkerTask, task_fn: Callable[[], Coroutine[Any, None, None]]
    ) -> WorkerTask:
        run_attempt = TaskRunAttempt(
            state=StateWithHistory(current=State.RUNNING), worker_id=self._id
        )
        attempts = deepcopy(task.task_run.attempts or [])
        attempts.append(run_attempt)
        worker_task_result = WorkerTaskResult(
            task_run=task.task_run.with_attempts(attempts)
        )
        await self._task_result_publisher(worker_task_result)
        asyncio_task = asyncio.create_task(task_fn())
        async with self._lock:
            self._cancellable.add(asyncio_task)
        # TODO: here we need to replicate the advanced error handling of kestra
        #  workers.
        #  For now we don't handle cancellation (aka stop) and kill
        outputs = dict()
        try:
            state, outputs = await self._run_with_timeout(task, asyncio_task)
        except Exception as e:  # noqa: BLE001
            state = await self.exception_handler(e)
        finally:
            async with self._lock:
                self._cancellable.remove(asyncio_task)
        run_attempt = TaskRunAttempt(
            state=StateWithHistory().with_state(state), worker_id=self._id
        )
        attempts = deepcopy(task.task_run.attempts or [])
        attempts.append(run_attempt)
        task = task.with_attempts(attempts).with_outputs(outputs)
        return task

    async def _run_with_timeout(
        self, task: WorkerTask, task_fn: asyncio.Task
    ) -> tuple[State, Any]:
        timeout_cm = _timeout_cm(task)
        state, outputs = None, None
        try:
            async with timeout_cm:
                outputs = await task_fn
                if (
                    isinstance(outputs, TaskOutputABC)
                    and outputs.final_state is not None
                ):
                    state = outputs.final_state
                else:
                    state = State.SUCCESS
        except asyncio.TimeoutError as e:
            self.error(
                "task %s run %s cancelled due to timeout",
                task.uid,
                task.task_run.id,
            )
            state = await self.exception_handler(e)
        except RunnableTaskException as e:
            outputs = e.output
            state = await self.exception_handler(e)
        return state, outputs

    async def exception_handler(self, exc: Exception) -> State:
        if self._killed:
            return State.KILLED
        # We just log the error in case of failure
        self.exception(_format_error(exc))
        return State.FAILED


def _timeout_cm(task: WorkerTask) -> AbstractAsyncContextManager:
    timeout_cm = do_nothing_cm()
    if task.task.timeout is not None:
        timeout = task.run_context.render(task.task.timeout, task.run_context.variables)
        timeout = _DURATION_TYPE_ADAPTER.validate_python(timeout)
        timeout_cm = asyncio.timeout(timeout.total_seconds())
    return timeout_cm


def _format_error(error: BaseException) -> str:
    return "".join(traceback.format_exception(None, error, error.__traceback__))


def _validate_task_output(task_fn: Callable) -> Callable:
    hints = get_type_hints(task_fn)
    return_type = hints.get("return")
    if return_type is None:
        raise ValueError("missing task function return type annotation")
    if return_type is NoneType:
        return task_fn
    if return_type is dict:
        return task_fn
    origin = get_origin(return_type)
    if origin is dict:
        return task_fn
    if isinstance(return_type, type) and issubclass(return_type, dict):
        return task_fn
    msg = (
        f"task function {task_fn.__name__} is expected to return a dict, "
        f"found {return_type}"
    )
    raise ValueError(msg)
