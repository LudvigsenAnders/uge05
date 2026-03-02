
# import os
# import psycopg
# from contextlib import contextmanager

# # Read connection string from environment variable
# DATABASE_URL = os.getenv(
#     "DATABASE_URL",
#     "host=localhost port=5432 dbname=northwind user=postgres password=password"
# )


# @contextmanager
# def get_db():
#     """Reusable synchronous DB connection generator."""
#     conn = None
#     try:
#         conn = psycopg.connect(DATABASE_URL)
#         yield conn
#     finally:
#         if conn:
#             conn.close()





# db.py
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import (AsyncEngine, AsyncSession, create_async_engine)
from sqlalchemy.orm import sessionmaker

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
async def get_session() -> AsyncSession:
    """
    Use this with 'async with' in any function that needs DB access.
    """
    async with AsyncSessionLocal() as session:
        yield session
