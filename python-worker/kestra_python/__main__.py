import asyncio
import logging
from copy import deepcopy

from icij_common.logging_utils import setup_loggers
from psycopg import AsyncClientCursor
from psycopg.conninfo import make_conninfo
from psycopg_pool import AsyncConnectionPool

from kestra_python.utils import make_worker_id
from kestra_python.worker import Worker

logger = logging.getLogger(__name__)


async def main_poll_python_worker_group_queue(
    connection_info: str,
    app_name: str,
    poll_size: int = 1,
    poll_interval_s: float = 0.1,
) -> None:
    worker_id = make_worker_id(app_name)
    kwargs = {"autocommit": True, "cursor_factory": AsyncClientCursor}
    connection_pool = AsyncConnectionPool(connection_info, kwargs=kwargs)
    async with connection_pool:
        worker = Worker(
            deepcopy(TASK_REGISTRY),
            connection_pool,
            worker_id,
            logger=logger,
            poll_size=poll_size,
            poll_interval_s=poll_interval_s,
        )
        await worker.work()


async def _hello_world(greeted: str) -> dict[str, str]:
    greeted_msg = f"hello {greeted}"
    logger.info("greeted: %s", greeted)
    return {"greeted": greeted_msg}


TASK_REGISTRY = {
    "hello_world": _hello_world,
}

if __name__ == "__main__":
    setup_loggers(["__main__", "kestra_python"], level=logging.DEBUG)
    conn_info = make_conninfo(
        host="127.0.0.1",
        password="admin",
        user="admin",
        dbname="postgres",
        port=5432,
        connect_timeout="2.0",
        sslmode="disable",
    )
    asyncio.run(main_poll_python_worker_group_queue(conn_info, app_name="sample_app"))
