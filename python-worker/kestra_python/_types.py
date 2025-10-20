import json
import logging
import random
import re
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from enum import Enum
from string import Template
from typing import Annotated, Any, Literal, LiteralString, Self, TypeVar

from icij_common.pydantic_utils import (
    icij_config,
    lowercamel_case_config,
    merge_configs,
    no_enum_values_config,
    safe_copy,
)
from jinja2 import Environment
from psycopg import sql
from pydantic import (
    AnyUrl,
    BaseModel,
    BeforeValidator,
    Field,
    model_validator,
)

Query = LiteralString | bytes | sql.SQL | sql.Composed | Template
Params = Sequence[Any] | Mapping[str, Any]
Record = TypeVar("Record", bound=BaseModel)

BASE_CONFIG = merge_configs(icij_config(), no_enum_values_config())
BASE_LOWER_CAMEL_CONFIG = merge_configs(BASE_CONFIG, lowercamel_case_config())

logger = logging.getLogger(__name__)


class BaseCamelCaseModel(BaseModel):
    model_config = BASE_LOWER_CAMEL_CONFIG


# TODO: generate some objects from the OpenAPI of kestra


class ExecutionKind(str, Enum):
    NORMAL = "NORMAL"
    TEST = "TEST"
    PLAYGROUND = "PLAYGROUND"


class ValueAndOffset(BaseModel):
    model_config = BASE_CONFIG

    value: dict[str, Any]
    offset: int


class WorkerJobABC(BaseCamelCaseModel, ABC):
    type: str


class State(str, Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    RESTARTED = "RESTARTED"
    KILLING = "KILLING"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    FAILED = "FAILED"
    KILLED = "KILLED"
    CANCELLED = "CANCELLED"
    QUEUED = "QUEUED"
    RETRYING = "RETRYING"
    RETRIED = "RETRIED"
    SKIPPED = "SKIPPED"
    BREAKPOINT = "BREAKPOINT"

    @property
    def is_failed(self) -> bool:
        return self is State.FAILED

    @classmethod
    def resolve_failure(cls, task: "Task") -> Self:
        if task.allow_failure and task.allow_warning:
            return cls.SUCCESS
        if task.allow_warning:
            return cls.WARNING
        return cls.FAILED


class WorkerGroupFallback(str, Enum):
    FAIL = "FAIL"
    WAIT = "WAIT"
    CANCEL = "CANCEL"


class WorkerGroup(BaseCamelCaseModel):
    key: str
    fallback: WorkerGroupFallback


class StateHistory(BaseCamelCaseModel):
    state: State
    date: datetime


class StateWithHistory(BaseCamelCaseModel):
    current: State = State.CREATED
    histories: list[StateHistory] = []
    duration: timedelta = timedelta(seconds=0)
    start_date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def _add_current_to_history(self) -> Self:
        if not self.histories or self.histories[-1].state != self.current:
            new_state = self.current
            histories = deepcopy(self.histories)
            now = datetime.now(timezone.utc)
            new_state_date = histories[-1].date if len(histories) > 1 else now
            histories.append(StateHistory(state=new_state, date=now))
            start_date = histories[0].date
            duration = new_state_date - start_date
            self.__dict__["histories"] = histories
            self.__dict__["duration"] = duration
            self.__dict__["start_date"] = start_date
        return self

    def with_state(self, state: State) -> Self:
        if state is self.current:
            logger.warning("Can't change state, already %s", state, stack_info=True)
            # This object is frozen we can return it without deepcopy
            return self
        return StateWithHistory(
            current=state,
            histories=self.histories,
            duration=self.duration,
            start_date=self.start_date,
        )


class TaskRunAttempt(BaseCamelCaseModel):
    state: StateWithHistory
    worker_id: str
    uri: AnyUrl | None = None

    @property
    def max_date(self) -> datetime:
        if not self.state.histories:
            return datetime.now(timezone.utc)
        return self.state.histories[-1].date


class RetryConfig(BaseCamelCaseModel):
    delay_factor: float
    max_delay: timedelta | None = None
    jitter: timedelta | None = None
    jitter_factor: float
    max_duration: timedelta | None = None
    max_retries: int
    # TODO: we might need to handle alias serialization
    max_random_duration: Annotated[timedelta, Field(alias="delayMax")] = None
    min_random_duration: Annotated[timedelta, Field(alias="delaymin")] = None


class TaskOutputABC(BaseCamelCaseModel, ABC):
    final_state: State | None = None


def _min_value_validator(
    min_value: int, *, strict: bool = True
) -> Callable[[int], int]:
    def _validator(value: int) -> min_value:
        if strict:
            if not value > min_value:
                raise ValueError(f"expected value {value} > {min_value}")
        elif not value >= min_value:
            raise ValueError(f"expected value {value} >= {min_value}")
        return value

    return _validator


class Behavior(str, Enum):
    RETRY_FAILED_TASK = "RETRY_FAILED_TASK"
    CREATE_NEW_EXECUTION = "CREATE_NEW_EXECUTION"


class RetryABC(BaseCamelCaseModel, ABC):
    type: str

    max_duration: timedelta | None = None
    max_attempts: Annotated[int, BeforeValidator(_min_value_validator(1))]
    warning_on_retry: bool = False
    behavior: Behavior = Behavior.RETRY_FAILED_TASK

    def next_retry_date(self, n_attempts: int, last_attempt: datetime) -> datetime: ...


class Constant(RetryABC):
    type: Literal["constant"] = "constant"
    interval: timedelta

    @abstractmethod
    def next_retry_date(self, n_attempts: int, last_attempt: datetime) -> datetime:
        return last_attempt + self.interval


class Exponential(RetryABC):
    type: Literal["exponential"] = "exponential"
    interval: timedelta
    max_interval: timedelta
    delay_factor: float = 2.0

    def next_retry_date(self, n_attempts: int, last_attempt: datetime) -> datetime:
        interval = self.interval * self.delay_factor * (n_attempts - 1)
        next_date = last_attempt + interval
        max_allowed = last_attempt + self.max_interval
        return min(max_allowed, next_date)


class Random(RetryABC):
    type: Literal["random"] = "random"
    min_interval: timedelta
    max_interval: timedelta

    def next_retry_date(self, n_attempts: int, last_attempt: datetime) -> datetime:  # noqa: ARG002
        interval = self.min_interval.total_seconds()
        interval += random.random() * (self.max_interval.total_seconds() - interval)
        interval = timedelta(seconds=interval)
        return last_attempt + interval


Retry = Annotated[Constant | Exponential | Random, Field(discriminator="type")]


class TaskRun(BaseCamelCaseModel):
    tenant_id: str
    id: str
    execution_id: str
    namespace: str
    flow_id: str
    task_id: str
    state: StateWithHistory
    parent_task_run_id: str | None = None
    value: str | None = None
    attempts: list[TaskRunAttempt] | None = None
    outputs: dict[str, Any] | None
    iteration: int | None = None
    dynamic: bool | None = None
    force_execution: bool | None = None

    @property
    def n_attempts(self) -> int:
        if self.attempts is None:
            return 0
        return len(self.attempts)

    def with_state(self, state: State) -> Self:
        return safe_copy(self, update={"state": self.state.with_state(state)})

    def with_attempts(self, attempts: list[TaskRunAttempt]) -> Self:
        return safe_copy(self, update={"attempts": attempts})

    def with_outputs(self, outputs: dict[str, Any]) -> Self:
        return safe_copy(self, update={"outputs": outputs})

    def should_be_retried(self, retry: Retry | None) -> bool:
        if retry is None:
            return False
        return self.next_retry_date(retry) is not None

    def next_retry_date(self, retry: Retry) -> datetime | None:
        if self.attempts is None or not self.attempts:
            return None
        if retry.max_attempts is not None and self.n_attempts >= retry.max_attempts:
            return None
        last_attempt = self.attempts[-1].max_date
        next_date = retry.next_retry_date(len(self.attempts), last_attempt)
        max_allowed = next_date + retry.max_duration
        if retry.max_duration is not None and next_date > max_allowed:
            return None
        return next_date


class Task(BaseCamelCaseModel):
    id: str
    type: str
    worker_group: WorkerGroup | None = None
    timeout: str | None = None
    retry: Retry | None = None
    allow_failure: bool = False
    allow_warning: bool = False


class ExternalTask(Task):
    name: str
    inputs: str | None = None


def _with_task_run_id(data: Any) -> Any:
    if isinstance(data, dict):
        task_run = data.get("taskRun", data.get("task_run"))
        if isinstance(task_run, TaskRun):
            data["uid"] = task_run.id
        elif isinstance(task_run, dict):
            data["uid"] = task_run["id"]
    return data


class WorkerTaskResult(BaseCamelCaseModel):
    uid: str | None = None
    task_run: TaskRun
    dynamic_task_runs: list[TaskRun] = []

    @model_validator(mode="before")
    @classmethod
    def with_task_run_id_as_uid(cls, data: Any) -> Any:
        return _with_task_run_id(data)


class RunContext(BaseCamelCaseModel):
    initialized: bool
    variables: dict[str, Any] | None
    secret_inputs: list[str] | None
    trace_parent: str | None = None
    dynamic_worker_results: list[WorkerTaskResult] = []

    _raw_pattern = re.compile(r"(\{%-*\s*raw\s*-*%}(.*?)\{%-*\s*endraw\s*-*%})")
    _jinja_env = Environment()

    def render(self, inputs: str, variables: dict[str, Any]) -> Any:
        if not inputs:
            return None
        if "{" not in inputs:
            return json.loads(inputs)
        matches = self._raw_pattern.search(inputs)
        rendered, raw_replacements = _replace_raw(inputs, matches)
        rendered = self._jinja_env.from_string(inputs).render(variables)
        for to_replace, replacement in raw_replacements.items():
            rendered = rendered.replace(to_replace, replacement)
        rendered = json.loads(rendered)
        if isinstance(rendered, str):
            rendered = self._raw_pattern.sub("$2", rendered)
        return rendered


class WorkerTask(WorkerJobABC):
    type: Literal["task"] = "task"

    uid: str
    task_run: TaskRun
    task: ExternalTask
    run_context: RunContext
    execution_kind: ExecutionKind | None = None

    @model_validator(mode="before")
    @classmethod
    def with_task_run_id_as_uid(cls, data: Any) -> Any:
        return _with_task_run_id(data)

    def with_state(self, state: State) -> Self:
        return safe_copy(self, update={"task_run": self.task_run.with_state(state)})

    def with_attempts(self, attempts: list[TaskRunAttempt]) -> Self:
        return safe_copy(
            self, update={"task_run": self.task_run.with_attempts(attempts)}
        )

    def fail(self) -> Self:
        failure_state = State.resolve_failure(self.task)
        return safe_copy(
            self, update={"task_run": self.task_run.with_state(failure_state)}
        )

    def with_outputs(self, outputs: dict[str, Any]) -> Self:
        return safe_copy(self, update={"task_run": self.task_run.with_outputs(outputs)})

    def resolve_final_state(self) -> Self:
        if not self.task_run.attempts:
            msg = (
                f"Can find lastAttempt on taskRun"
                f" {self.task_run.model_dump_json(by_alias=True)}"
            )
            raise ValueError(msg)
        attempt = self.task_run.attempts[-1]
        state = attempt.state.current
        if (
            state is State.SUCCESS
            and self.task.retry is not None
            and self.task.retry.warning_on_retry
            and self.task_run.n_attempts > 1
        ):
            state = State.WARNING
        if (
            state.is_failed
            and self.task.allow_failure
            and not self.task_run.should_be_retried(self.task.retry)
        ):
            state = State.WARNING
        if self.task.allow_warning and state is State.WARNING:
            state = State.SUCCESS
        return safe_copy(self, update={"task_run": self.task_run.with_state(state)})


class WorkerTrigger(WorkerJobABC):
    type: Literal["trigger"] = "trigger"


def _replace_raw(inputs: str, matches: list[re.Match]) -> tuple[str, dict[str, str]]:
    if not matches:
        return inputs, dict()
    replacements = dict()
    replaced = ""
    for match in matches:
        raw_value = match.group(0)
        key = uuid.uuid4().hex
        replacements[key] = raw_value
        replaced += key
    replaced += inputs[matches[-1].end() :]
    return replaced, replacements


WorkerJob = Annotated[WorkerTask | WorkerTrigger, Field(discriminator="type")]
