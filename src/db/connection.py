
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import (AsyncEngine, AsyncSession, create_async_engine)
from sqlalchemy.orm import sessionmaker
from typing import AsyncGenerator

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# -----------------------------------------------------------
# Create async SQLAlchemy engine (with asyncpg + pooling)
# -----------------------------------------------------------
engine: AsyncEngine = create_async_engine(
    DATABASE_URL,
    echo=False,        # set to True for SQL logging
    future=True,
    pool_size=10,      # max number of open connections
    max_overflow=20,   # extra temporary connections
)

# -----------------------------------------------------------
# Create async session factory
# -----------------------------------------------------------
AsyncSessionLocal = sessionmaker(
    engine,
    expire_on_commit=False,
    class_=AsyncSession,
)


# -----------------------------------------------------------
# Dependency / helper to get an async session
# -----------------------------------------------------------
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Use this with 'async with' in any function that needs DB access.
    """
    async with AsyncSessionLocal() as session:
        yield session
