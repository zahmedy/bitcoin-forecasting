import os
from functools import lru_cache
from sqlalchemy import create_engine


def _normalize_db_url(db_url: str) -> str:
    if db_url.startswith("postgresql://"):
        return db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return db_url


@lru_cache(maxsize=1)
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return create_engine(_normalize_db_url(db_url), future=True)
