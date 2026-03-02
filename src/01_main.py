#from unittest import result


import asyncio
from sqlalchemy import text
from db.connection import get_session


async def main():
    async for session in get_session():
        result = await session.execute(text("SELECT NOW();"))
        print(result.scalar())
        result = await session.execute(
            text("SELECT * FROM employees WHERE country = :c"), {"c": "UK"})
        rows = result.mappings().all()  # dict-like rows
        for row in rows:
            print(row)


asyncio.run(main())
