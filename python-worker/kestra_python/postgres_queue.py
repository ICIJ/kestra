import asyncio
import functools
import json
import uuid
from collections.abc import AsyncIterable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from typing import Any, LiteralString, cast

from psycopg import AsyncConnection, sql
from psycopg.errors import DataError
from psycopg.rows import class_row
from psycopg.types.json import Jsonb
from pydantic import TypeAdapter
from pydantic_core import to_jsonable_python

from kestra_python._types import Record, ValueAndOffset
from kestra_python.exceptions import (
    MessageTooBigException,
    QueueException,
    UnsupportedMessageException,
)
from kestra_python.utils import encode_base_62

CONSUMER_COL = "consumer"
CONSUMER_GROUP_COL = "consumer_group"
KEY_COL = "key"
QUEUE_TABLE = "queues"
QUEUE_TYPE_COL = "type"
LIMIT = "limit"
OFFSET_COL = "offset"
OFFSETS = "queue_offsets"
UPDATED_COL = "updated"
VALUE = "value"


class ConsumerType(str, Enum):
    INDEXER = "index"
    EXECUTOR = "executor"
    WORKER = "worker"
    SCHEDULER = "scheduler"


class QueueType(str, Enum):
    EXECUTION = "io.kestra.core.models.executions.Execution"
    FLOW = "io.kestra.core.models.flows.Flow"
    TEMPLATE = "io.kestra.core.models.templates.Template"
    EXECUTION_KILLED = "io.kestra.core.models.executions.ExecutionKilled"
    WORKER_JOB = "io.kestra.core.runners.WorkerJob"
    WORKER_TASK_RESULT = "io.kestra.core.runners.WorkerTaskResult"
    WORKER_INSTANCE = "io.kestra.core.runners.WorkerInstance"
    WORKER_TASK_RUNNING = "io.kestra.core.runners.WorkerTaskRunning"
    LOG_ENTRY = "io.kestra.core.models.executions.LogEntry"
    TRIGGER = "io.kestra.core.models.triggers.Trigger"


async def emit(
    msg: Any,
    conn: AsyncConnection,
    *,
    queue_type: QueueType,
    size_limit: int,
    consumer_group: str | None = None,
) -> None:
    if not conn.autocommit:
        raise ValueError("expected an autocommit connection")
    serialized = to_jsonable_python(msg, by_alias=True)
    serialized_as_bytes = json.dumps(serialized).encode()
    _check_size_limits(len(serialized_as_bytes), size_limit)
    msg_key = _message_id(msg)
    if msg_key is None:
        msg_key = encode_base_62(int(uuid.uuid4()))
    updated_fields = _produce_fields(
        queue_type, consumer_group, serialized, msg_key=msg_key
    )
    insert_query = _insert_query(QUEUE_TABLE, tuple(updated_fields.keys()))
    try:
        async with conn.cursor() as cur, conn.transaction():
            # TODO: handle indexing here if needed indexer.index(msg_bytes)
            await cur.execute(insert_query, updated_fields)
    except DataError as e:
        msg = str(e)
        # TODO: check that this check is correct
        if "ERROR: unsupported Unicode escape sequence" in msg:
            raise UnsupportedMessageException(msg) from e
        raise QueueException("Unable to emit a message to the queue") from e


async def poll_queue(
    conn: AsyncConnection,
    record_type: type[Record],
    poll_interval_s: float,
    *,
    consumer_group: str | None,
    queue_type: QueueType,
    consumer_type: ConsumerType,
    poll_size: int = 1,
    for_update: bool = True,
) -> AsyncIterable[list[Record]]:
    fetch_record_tx = functools.partial(
        _fetch_from_queue_tx,
        conn,
        queue_type=queue_type,
        consumer_type=consumer_type,
        record_type=record_type,
        poll_size=poll_size,
        for_update=for_update,
        consumer_group=consumer_group,
    )
    async for rec in poll(fetch_record_tx, poll_interval_s=poll_interval_s):
        yield rec


async def poll(
    fetch_record_tx: Callable[[], AbstractAsyncContextManager[Record]],
    *,
    poll_interval_s: float,
) -> AsyncIterable[list[Record]]:
    while True:
        async with fetch_record_tx() as polled:
            if polled:
                yield polled
        if not polled:
            # TODO: handle min_max_poll_duration, exponential increase, etc...
            await asyncio.sleep(poll_interval_s)


def _message_id(obj: Any) -> str | None:
    if hasattr(obj, "uid"):
        return obj.uid
    if hasattr(obj, "id"):
        return obj.id
    raise NotImplementedError(f"message_id is not implemented for {type(obj)}")


def _produce_fields(
    queue_type: QueueType, consumer_group: str | None, value: Any, *, msg_key: str
) -> dict[str, Any]:
    fields = {QUEUE_TYPE_COL: queue_type, VALUE: Jsonb(value), KEY_COL: msg_key}
    if consumer_group is not None:
        fields[CONSUMER_GROUP_COL] = consumer_group
    return fields


def _check_size_limits(msg_size: int, size_limit: int) -> None:
    if size_limit < msg_size:
        raise MessageTooBigException(msg_size, size_limit)


@asynccontextmanager
async def _fetch_from_queue_tx(
    conn: AsyncConnection,
    queue_type: QueueType,
    consumer_type: ConsumerType,
    record_type: type[Record],
    *,
    poll_size: int = 1,
    for_update: bool = True,
    in_transaction: bool = True,
    consumer_group: str | None,
) -> AsyncIterable[list[Record]]:
    if not conn.autocommit:
        raise ValueError("expected an autocommit connection")
    row_factory = class_row(ValueAndOffset)
    select_query = _select_queue_record_query(
        QUEUE_TABLE, consumer_type, for_update=for_update, consumer_group=consumer_group
    )
    select_params = {LIMIT: poll_size, QUEUE_TYPE_COL: queue_type.value}
    if consumer_group is not None:
        select_params["consumer_group"] = consumer_group
    update_query = _update_consumer_offsets_query(
        QUEUE_TABLE, consumer_type, consumer_group=consumer_group
    )
    type_adapter = TypeAdapter(record_type)
    async with conn.cursor(row_factory=row_factory) as cur:
        async with conn.transaction():
            await cur.execute(select_query, select_params)
            recs = await cur.fetchall()
            if not recs:
                yield []
                return
            offsets = [r.offset for r in recs]
            recs = [type_adapter.validate_python(r.value) for r in recs]
            yield recs
            if recs and in_transaction:
                updated_params = {
                    UPDATED_COL: datetime.now(timezone.utc),
                    OFFSETS: offsets,
                }
                if consumer_group is not None:
                    updated_params["consumer_group"] = consumer_group
                await cur.execute(update_query, updated_params)
                return
        if recs:
            updated_params = {UPDATED_COL: datetime.now(timezone.utc), OFFSETS: offsets}
            if consumer_group is not None:
                updated_params["consumer_group"] = consumer_group
            await cur.execute(update_query, updated_params)


@lru_cache
def _insert_query(table: str, colunms: tuple[str, ...]) -> sql.Composed:
    col_names = sql.SQL(", ").join(sql.Identifier(n) for n in colunms)
    col_value_placeholders = sql.SQL(", ").join(sql.Placeholder(n) for n in colunms)
    query = sql.SQL(_INSERT_INTO_QUEUE).format(
        col_names,
        col_value_placeholders,
        queue_table=sql.Identifier(table),
        queue_type=sql.Identifier(QUEUE_TYPE_COL),
    )
    return query


@lru_cache
def _select_queue_record_query(
    table: str,
    consumer_type: ConsumerType,
    *,
    consumer_group: str | None,
    disable_seq_scan: bool = False,
    for_update: bool = True,
) -> sql.Composed:
    queries = []
    if disable_seq_scan:
        queries.append(_DISABLE_SEQ_SCAN)
    if for_update:
        queries.append(_select_from_queue_query(consumer_group))
    queries.extend([_ORDER_BY_ASC_OFFSET, _LIMIT])
    return sql.SQL("\n".join(queries)).format(
        queue_table=sql.Identifier(table),
        queue_type=sql.Identifier(QUEUE_TYPE_COL),
        value=sql.Identifier(VALUE),
        offset=sql.Identifier(OFFSET_COL),
        consumer_group=sql.Identifier(CONSUMER_GROUP_COL),
        consumer_type=sql.Identifier(CONSUMER_COL + "_" + consumer_type.value),
    )


def _select_from_queue_query(consumer_group: str | None) -> str:
    if consumer_group is None:
        return _SELECT_FROM_QUEUE_QUERY + " AND q.{consumer_group} IS NULL"
    return _SELECT_FROM_QUEUE_QUERY + " AND q.{consumer_group} = %(consumer_group)s"


@lru_cache
def _update_consumer_offsets_query(
    table: str, consumer_type: ConsumerType, *, consumer_group: str | None
) -> sql.Composed:
    query = _UPDATE_QUEUE_GROUP_OFFSETS_QUERY
    if consumer_group is None:
        query += " AND {consumer_group} IS NULL"
    else:
        query += " AND {consumer_group} = %(consumer_group)s"
    return sql.SQL(query).format(
        queue_table=sql.Identifier(table),
        consumer_group=sql.Identifier(CONSUMER_GROUP_COL),
        consumer_type=sql.Identifier(CONSUMER_COL + "_" + consumer_type.value),
        updated=sql.Identifier(UPDATED_COL),
        queue_offset=sql.Identifier(OFFSET_COL),
    )


_DISABLE_SEQ_SCAN = "SET LOCAL enable_seqscan TO off"

_INSERT_INTO_QUEUE = cast(LiteralString, "INSERT INTO {queue_table} ({}) VALUES ({})")

_SELECT_FROM_QUEUE_QUERY = """SELECT q.{value}, q.{offset}
FROM {queue_table} AS q
WHERE q.{queue_type} = CAST(%(type)s AS queue_type)
    AND q.{consumer_type} IS FALSE
"""

_UPDATE_QUEUE_GROUP_OFFSETS_QUERY = cast(
    LiteralString,
    """UPDATE {queue_table}
SET {consumer_type} = TRUE, {updated} = %(updated)s
WHERE {queue_offset} = ANY(%(queue_offsets)s)
""",
)

_ORDER_BY_ASC_OFFSET = "ORDER BY q.offset ASC"
_LIMIT = "LIMIT %(limit)s"
_FOR_UPDATE_SKIP_LOCKED = "FOR UPDATE SKIP LOCKED"
