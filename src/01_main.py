import psycopg
import db.connection as connection



with connection.get_db() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM customers")
        rows = cur.fetchall()
        print(rows)




conn_str = (
    "host=localhost port=5432 dbname=northwind user=postgres password=password"
    # "postgresql://postgres:password@localhost:5432/northwind"
)

# Using context manager auto-commits or rolls back on exception
with psycopg.connect(conn_str) as conn:
    with conn.cursor() as cur:
        # Use parameterized queries to avoid SQL injection
        cur.execute("SELECT id, name FROM customers WHERE country = %s", ("DK",))
        rows = cur.fetchall()
        for r in rows:
            print(r)


##################
##################


import asyncio
from db import get_session

async def main():
    async for session in get_session():
        result = await session.execute(
            "SELECT NOW();"
        )
        print(result.scalar())

asyncio.run(main())
