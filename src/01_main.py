import asyncio
from sqlalchemy import text
from db.connection import get_session, close_engine


# async def main():
#     async for session in get_session():
#         result = await session.execute(text("SELECT NOW();"))
#         print(result.scalar())
#         result = await session.execute(
#             text("SELECT * FROM employees WHERE country = :c"), {"c": "UK"})
#         rows = result.mappings().all()  # dict-like rows
#         for row in rows:
#             print(row)
#     await close_engine()

# asyncio.run(main())



from sqlalchemy import select
from models import employees  # ORM model with __tablename__ = "employees"

async def main():
    async for session in get_session():
        stmt = select(employees).where(employees.c.country == "UK")
        result = await session.execute(stmt)
        employees = result.scalars().all()  # returns list[Employee]
        for e in employees:
            print(e.id, e.name)
asyncio.run(main())