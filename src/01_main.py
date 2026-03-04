import asyncio
import datetime
from sqlalchemy import text
from db.connection import get_session, close_engine
import db.db_utils as db_utils
from sqlalchemy import select
from sqlalchemy import text, bindparam


async def main():
    async for session in get_session():
        q = db_utils.Query(session)
        # Get a single scalar
        now = await q.fetch_value("SELECT NOW()")
        print("Now:", now)

        # Get one row
        one_row = await q.fetch_one(
            "SELECT * FROM employees"
        )
        print("One row:", one_row)

        # Get rows
        employees = await q.fetch_all(
            "SELECT * FROM employees",
            as_mapping=True
        )
        print("All employees:", employees)

        # Get rows
        UK_employees = await q.fetch_all(
            "SELECT * FROM employees WHERE country IN :countries",
            {"countries": ["UK"]},
            as_mapping=True
        )
        print("UK employees:", UK_employees)

        print(await q.exists(
        "SELECT 1 FROM employees WHERE employeeid = :id",
        {"id": 10}
        ))
        print(await q.exists(
        "SELECT 1 FROM employees WHERE employeeid = :id",
        {"id": 15}
        ))
        print(await q.count(
        "SELECT COUNT(*) FROM employees WHERE country = :c",
        {"c": "Germany"}
        ))
        print(await q.count(
        "SELECT COUNT(*) FROM employees WHERE country = :c",
        {"c": "UK"}
        ))

        # Insert inside a transaction
        async with q.transaction():
            new_emp = await q.insert(
                "employees",
                {
                    'lastname': 'MyLastname',
                    'firstname': 'MyName',
                    'title': 'MyTitle',
                    'titleofcourtesy': 'Mr.',
                    'birthdate': datetime.date.fromisoformat("1966-01-27"),
                    'hiredate': datetime.date.fromisoformat("1993-10-17"),
                    'address': 'MyPlace 123',
                    'city': 'My City',
                    'region': None,
                    'postalcode': 'RG1 9SP',
                    'country': 'DK'
                },
                returning="*"
            )

        print("Inserted employee:", new_emp)

        # Get rows
        new_employees = await q.fetch_all(
            "SELECT * FROM employees WHERE employeeid = 11",
            as_mapping=True
        )
        print("All employees:", new_employees)

    await close_engine()




# print("Inserted employee ID:", id)

# rows = await q.update(
#     "employees",
#     {"age": 34},
#     where="id = :id",
#     params={"id": 10}
# )

# print("Rows updated:", rows


# country = await q.update(
#     "employees",
#     {"country": "Denmark"},
#     where="id = :id",
#     params={"id": 10},
#     returning="country"
# )

# deleted = await q.delete(
#     "employees",
#     where="id = :id",
#     params={"id": 10}
# )

# email = await q.delete(
#     "employees",
#     where="id = :id",
#     params={"id": 11},
#     returning="email"
# )


if __name__ == "__main__":
    asyncio.run(main())
