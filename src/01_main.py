import asyncio
import datetime
import pandas as pd
import matplotlib.pyplot as plt
from db.connection import get_session, close_engine
from db.db_utils import QueryRunner


async def main():
    async for session in get_session():
        q = QueryRunner(session)
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
            {"countries": ["UK", "USA"]},
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
            "SELECT COUNT(*) FROM employees"
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

            print(await q.count(
                "SELECT COUNT(*) FROM employees"
            ))

            await q.update(
                "employees",
                {"firstname": "NewName"},
                where="employeeid = :id",
                params={"id": 10}
            )

            await q.delete(
                "employees",
                where="lastname = :lname",
                params={"lname": "MyLastname"},
                returning="*"
            )

        print(await q.count(
            "SELECT COUNT(*) FROM employees"
        ))

        products_limit_5 = await q.fetch_all(
            "SELECT * FROM products LIMIT 5",
            {},
            as_mapping=True
        )
        print("Products (limited to 5):", products_limit_5)

        df_employees = await q.dataframe("SELECT * FROM employees")
        df_orders = await q.dataframe("SELECT * FROM orders")
        df_orderdetails = await q.dataframe("SELECT * FROM orderdetails")
        df_products = await q.dataframe("SELECT * FROM products")
        df_customers = await q.dataframe("SELECT * FROM customers")

    await close_engine()
    print(df_employees.info())
    print(df_orders.info())
    print(df_orderdetails.info())
    print(df_products.info())
    print(df_customers.info())

    # prepare orderdetails for calculations (convert to numeric, handle missing/invalid)
    df_orderdetails["quantity"] = pd.to_numeric(df_orderdetails["quantity"], errors="coerce").fillna(0).astype(int)
    df_orderdetails["unitprice"] = pd.to_numeric(df_orderdetails["unitprice"], errors="coerce")  # float
    df_orderdetails["discount"] = pd.to_numeric(df_orderdetails["discount"], errors="coerce")  # float

    df = df_orderdetails.merge(df_orders, on="orderid").merge(df_customers, on="customerid")
    print(df.info())

    df["line_total"] = df["unitprice"] * df["quantity"] * (1 - df.get("discount", 0))

    revenue_per_country = (
        df.groupby(["country"])
        .agg(revenue=("line_total", "sum"))
        .sort_values("revenue", ascending=False)
    )
    revenue_per_country.plot(kind="bar", title="Revenue per country")
    plt.xlabel("Country")
    plt.ylabel("Revenue")
    plt.tight_layout()
    revenue_per_orders = df.groupby(["orderid"]).agg(revenue=("line_total", "sum"))
    revenue_per_orders.plot(kind="hist", bins=50, title="Revenue per order")
    plt.xlabel("Order revenue")
    plt.ylabel("Number of orders")
    plt.tight_layout()
    revenue_per_customer = (
        df.groupby(["customerid", "companyname"])
        .agg(revenue=("line_total", "sum"))
        .sort_values("revenue", ascending=False)
        .head(20)
    )
    revenue_per_customer.plot(kind="bar", title="Revenue per customer")
    plt.xlabel("Customer")
    plt.ylabel("Revenue")
    plt.tight_layout()
    revenue_per_employee = (
        df.merge(df_employees, on="employeeid")
        .groupby(["employeeid", "lastname", "firstname"])
        .agg(revenue=("line_total", "sum"))
        .sort_values("revenue", ascending=False)
    )
    revenue_per_employee.plot(kind="bar", title="Revenue per employee")
    plt.xlabel("Employee")
    plt.ylabel("Revenue")
    plt.tight_layout()

    plt.show()

if __name__ == "__main__":
    asyncio.run(main())
