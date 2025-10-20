import logging
import os
import socket
import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

BASE62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


class WithNameLoggingAdapter(logging.LoggerAdapter):
    def __init__(self, logger: logging.Logger, name: str, *, name_label: str) -> None:
        self._name = name
        self._name_label = name_label
        super().__init__(logger)

    def process(
        self, msg: str, kwargs: dict[str, object]
    ) -> tuple[str, dict[str, object]]:
        return f"[{self._name_label}-{self._name}]: {msg}", kwargs


def make_worker_id(app_name: str) -> str:
    pid = os.getpid()
    threadid = threading.get_ident()
    hostname = socket.gethostname()
    # TODO: this might not be unique when using asyncio
    return f"{app_name}-worker-{hostname}-{pid}-{threadid}"


def encode_base_62(num: int) -> str:
    if num == 0:
        return BASE62[0]
    arr = []
    arr_append = arr.append
    _divmod = divmod  # Access to locals is faster.
    base = len(BASE62)
    while num:
        num, rem = _divmod(num, base)
        arr_append(BASE62[rem])
    arr.reverse()
    return "".join(arr)


def decode_base_62(s: str) -> int:
    base = len(BASE62)
    strlen = len(s)
    num = 0

    for idx, char in enumerate(s):
        power = strlen - (idx + 1)
        num += BASE62.index(char) * (base**power)

    return num


@asynccontextmanager
async def do_nothing_cm() -> AsyncGenerator[None, None]:
    yield
