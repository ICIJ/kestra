from typing import Any

from kestra_python._types import WorkerTask


class QueueException(Exception): ...


class MessageTooBigException(QueueException):
    def __init__(self, size: int, max_size: int) -> None:
        msg = f"Message of size {size} has exceeded the configured limit of {max_size}"
        super().__init__(msg)


class UnsupportedMessageException(QueueException): ...


class UnknownTask(Exception):
    def __init__(self, task: WorkerTask):
        super().__init__(f"Unknown task name: {task.task.name} for task {task.task.id}")


class RunnableTaskException(Exception):
    def __init__(self, output: Any, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = output
