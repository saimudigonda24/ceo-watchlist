# api/app/db.py
from __future__ import annotations
import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase

# Use .env if present, otherwise fall back to a local dev DB
DATABASE_URL = os.environ.get("DATABASE_URL") or "sqlite+aiosqlite:///./data/dev.db"

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

class Base(AsyncAttrs, DeclarativeBase):
    pass

async def get_session():
    async with SessionLocal() as session:
        yield session