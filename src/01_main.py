import asyncio
import datetime
import pandas as pd
import matplotlib.pyplot as plt
from db.connection import get_session, close_engine, stream, stream_batches
from db.db_utils import QueryRunner


async def main():
    async for session in get_session():
        q = QueryRunner(session)
        # Get a single scalar
        now = await q.fetch_value("SELECT NOW()")
        print("Now:", now)

        # Get one row
        one_row = await q.fetch_one(
            "SELECT * FROM orderdetails"
        )
        print("One row:", one_row, "\n")

        # Get rows
        employees = await q.fetch_all(
            "SELECT firstname FROM employees",
            as_mapping=False
        )
        print("All employees:", "\n", employees, "\n")

        # Get rows with IN params
        customers = await q.fetch_all(
            "SELECT customerid, companyname FROM customers WHERE country IN :countries",
            {"countries": ["UK", "USA"]},
            as_mapping=True
        )
        print("UK and USA customers:", "\n", customers, "\n")

        # Get exists True/False
        print("Exists: ",
              await q.exists(
                  "SELECT 1 FROM employees WHERE employeeid = :id",
                  {"id": 10}
              ))
        # Get count of rows
        print("row count: ",
              await q.count(
                  "SELECT COUNT(*) FROM employees WHERE country = :c",
                  {"c": "UK"}
              ))

        async with q.transaction():
            # Insert inside a transaction
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
                returning="lastname"
            )
            print("Inserted employee: ", new_emp, "\n")

            # Update inside a transaction
            new_employee = await q.fetch_all(
                "SELECT * FROM employees WHERE employeeid = (SELECT MAX(employeeid) FROM employees)",
                as_mapping=True
            )
            print("New employee:", new_employee, "\n")

            await q.update(
                "employees",
                {"firstname": "ANDERS"},
                where="employeeid = (SELECT MAX(employeeid) FROM employees)"
            )

            new_employee = await q.fetch_all(
                "SELECT * FROM employees WHERE employeeid = (SELECT MAX(employeeid) FROM employees)",
                as_mapping=True
            )
            print("New employee:", new_employee, "\n")

            # Delete inside a transaction
            print("Count before delete: ", await q.count(
                "SELECT COUNT(*) FROM employees"
            ), "\n")

            await q.delete(
                "employees",
                where="lastname = :lname",
                params={"lname": "MyLastname"},
                returning="*"
            )

            print("Count after delete: ", await q.count(
                "SELECT COUNT(*) FROM employees"
            ), "\n")

        products_limit_5 = await q.fetch_all(
            "SELECT productname FROM products LIMIT 5",
            {},
            as_mapping=True
        )
        print("Products: ", products_limit_5)

        df_employees = await q.dataframe("SELECT * FROM employees")
        df_orders = await q.dataframe("SELECT * FROM orders")
        df_orderdetails = await q.dataframe("SELECT * FROM orderdetails")
        df_customers = await q.dataframe("SELECT * FROM customers")

    await close_engine()

    # prepare orderdetails for calculations (convert to numeric, handle missing/invalid)
    df_orderdetails["quantity"] = pd.to_numeric(df_orderdetails["quantity"], errors="coerce").fillna(0).astype(int)
    df_orderdetails["unitprice"] = pd.to_numeric(df_orderdetails["unitprice"], errors="coerce")  # float
    df_orderdetails["discount"] = pd.to_numeric(df_orderdetails["discount"], errors="coerce")  # float
    df = df_orderdetails.merge(df_orders, on="orderid").merge(df_customers, on="customerid")
    df["line_total"] = df["unitprice"] * df["quantity"] * (1 - df.get("discount", 0))

    # Visualize revenue by country, order, customer, employee
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
    revenue_per_employee.plot(kind="pie", title="Revenue per employee", y="revenue")

    plt.show()

    async for row in stream("SELECT * FROM orderdetails LIMIT 10"):
        print(row)

    async for batch in stream_batches("SELECT * FROM orderdetails", batch_size=1500):
        batch_df = pd.DataFrame(batch)
        print(batch_df.info())


if __name__ == "__main__":
    asyncio.run(main())
