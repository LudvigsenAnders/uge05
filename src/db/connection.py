
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import (AsyncEngine, AsyncSession, create_async_engine)
from sqlalchemy.orm import sessionmaker
from typing import AsyncGenerator

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

print("[DB] Loading DB module...")


# -----------------------------------------------------------
# Create async SQLAlchemy engine (with asyncpg + pooling)
# -----------------------------------------------------------
def _create_async_engine(database_url: str) -> AsyncEngine:
    """
    Create an async SQLAlchemy engine using the asyncpg driver.
    """
    print("[DB] Creating async engine (connection pool starts here)...")
    engine: AsyncEngine = create_async_engine(
        database_url,
        echo=True,        # set to True for SQL logging
        future=True,
        pool_size=10,      # max number of open connections
        max_overflow=20,   # extra temporary connections
    )
    return engine


engine: AsyncEngine = _create_async_engine(DATABASE_URL)

# -----------------------------------------------------------
# Create async session factory
# -----------------------------------------------------------
AsyncSessionLocal = sessionmaker(
    engine,
    expire_on_commit=False,
    class_=AsyncSession,
)


# -----------------------------------------------------------
# Dependency / helper to get a session
# -----------------------------------------------------------
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    print("[DB] Opening a new async SQLAlchemy session...")

    async with AsyncSessionLocal() as session:
        print("[DB] Session opened. Acquiring DB connection from pool...")

        try:
            yield session
        finally:
            print("[DB] Session closing… Connection returned to pool.")


# -----------------------------------------------------------
# Shutdown helper (optional)
# -----------------------------------------------------------
async def close_engine():
    print("[DB] Disposing engine... Closing all pooled connections.")
    await engine.dispose()
