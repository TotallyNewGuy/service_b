import os
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine


async_engine = None
engine = None


def init_db():
    global async_engine
    global engine
    if async_engine is None:
        async_engine = create_async_engine(
            os.getenv("DB_URL"), pool_size=10, max_overflow=0, pool_pre_ping=False, pool_recycle=1800
        )
    if engine is None:
        engine = create_engine(
            os.getenv("SYNC_URL"), pool_size=10, max_overflow=0, pool_pre_ping=False, pool_recycle=1800
        )


def get_async_engine():
    return async_engine


def get_db_engine():
    return engine