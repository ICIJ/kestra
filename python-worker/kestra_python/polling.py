import asyncio
import functools
from collections.abc import AsyncIterable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from typing import LiteralString, cast

from psycopg import AsyncConnection, sql
from psycopg.rows import class_row
from pydantic import TypeAdapter

from kestra_python._types import Record, ValueAndOffset

CONSUMER = "consumer"
CONSUMER_GROUP = "consumer_group"
QUEUES = "queues"
QUEUE_TYPE = "queue_type"
LIMIT = "limit"
OFFSET = "offset"
OFFSETS = "queue_offsets"
UPDATED = "updated"
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
        QUEUES, consumer_type, for_update=for_update, consumer_group=consumer_group
    )
    select_params = {LIMIT: poll_size, QUEUE_TYPE: queue_type.value}
    if consumer_group is not None:
        select_params["consumer_group"] = consumer_group
    update_query = _update_consumer_offsets_query(
        QUEUES, consumer_type, consumer_group=consumer_group
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
                updated_params = {UPDATED: datetime.now(timezone.utc), OFFSETS: offsets}
                if consumer_group is not None:
                    updated_params["consumer_group"] = consumer_group
                await cur.execute(update_query, updated_params)
                return
        if recs:
            updated_params = {UPDATED: datetime.now(timezone.utc), OFFSETS: offsets}
            if consumer_group is not None:
                updated_params["consumer_group"] = consumer_group
            await cur.execute(update_query, updated_params)


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
        value=sql.Identifier(VALUE),
        offset=sql.Identifier(OFFSET),
        consumer_group=sql.Identifier(CONSUMER_GROUP),
        consumer_type=sql.Identifier(CONSUMER + "_" + consumer_type.value),
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
        consumer_group=sql.Identifier(CONSUMER_GROUP),
        consumer_type=sql.Identifier(CONSUMER + "_" + consumer_type.value),
        updated=sql.Identifier(UPDATED),
        queue_offset=sql.Identifier(OFFSET),
    )


_DISABLE_SEQ_SCAN = "SET LOCAL enable_seqscan TO off"

_SELECT_FROM_QUEUE_QUERY = """SELECT q.{value}, q.{offset}
FROM {queue_table} AS q
WHERE q.type = CAST(%(queue_type)s AS queue_type)
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
