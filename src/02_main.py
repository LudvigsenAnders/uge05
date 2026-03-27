import asyncio
from db.connection import get_session, close_engine, inspect_pool
from db.db_utils import QueryRunner
from sqlalchemy import text


async def main():
    async for session in get_session():
        q = QueryRunner(session)
        async with q.transaction():
            # Get a single scalar
            now = await q.fetch_value("SELECT NOW()")
            print(f"Now: {now}", "\n")

            # Get one row
            one_row = await q.fetch_one(
                "SELECT * FROM orderdetails"
            )
            print(f"One row: {one_row}", "\n")

            await inspect_pool(session)

            await q.session.execute(text("SET ROLE pg_database_owner;"))
            emp = await q.execute_sql_file("src/sql/stored_procedures/create_get_employee_function.sql")
            emp = await q.execute_sql_file("src/sql/stored_procedures/grant_employee_function.sql")
            await q.session.execute(text("RESET ROLE;"))

            await q.session.execute(text("SET ROLE user_service_admin;"))
            result1 = await q.fetch_all("SELECT * FROM public.employees;")
            print(result1)
            result2 = await q.fetch_all("SELECT * FROM public.get_employee('Nancy');")
            print(result2)

            await q.session.execute(text("RESET ROLE;"))

            print(emp)

    await close_engine()
    await inspect_pool(session)

    print("Done!")

if __name__ == "__main__":
    asyncio.run(main())
