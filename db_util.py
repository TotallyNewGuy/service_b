import os
from sqlalchemy.ext.asyncio import create_async_engine


async_engine = None


def init_db():
    global async_engine
    if async_engine is None:
        async_engine = create_async_engine(
            os.getenv("DB_URL"), pool_size=10, max_overflow=0, pool_pre_ping=False, pool_recycle=1800
        )


def get_db_engine():
    return async_engine