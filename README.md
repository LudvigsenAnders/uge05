# Async PG/SQLAlchemy Utility (uge05)

This repository contains an asynchronous database utility built on top of **SQLAlchemy** and **asyncpg**, together with a small demo script that exercises the helpers and produces basic analytics using **pandas** and **matplotlib**. The code is intended as a learning/example project for interacting with a PostgreSQL copy of the Northwind Foods sample database asynchronously.

---

## Project Overview

The core of the project is the `QueryRunner` class in `src/db/db_utils.py` which provides convenient async methods for common SQL operations (fetch, insert, update, delete, bulk operations, transactions, streaming, etc.).

The `src/db/connection.py` module handles engine/session creation and includes helpers for asyncpg streams. The script `src/01_main.py` demonstrates usage of the utilities and performs some simple data analysis/visualisations.

## Features

- Async SQLAlchemy engine with asyncpg connection pooling
- `QueryRunner` for single queries or common patterns
- Automatic expansion of `IN` parameters for lists/tuples
- Transaction context manager with automatic rollback/commit
- Streaming queries one row at a time or in batches via asyncpg
- Helpers to return results as pandas `DataFrame`
- Demo analytics and plots from Northwind tables

## Getting Started

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd uge05
   ```

2. **Create and activate a virtual environment**
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1  # Windows PowerShell
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure database connection**
   Create a `.env` file in the project root with the following variables:
   ```ini
   SQLALCHEMY_URL=postgresql+asyncpg://user:password@host:port/dbname
   ASYNC_PG_URL=postgresql://user:password@host:port/dbname
   ```
   Replace credentials and host information as appropriate.

5. **Load the Northwind database** (optional demo data)
   You can use the SQL script in `data/northwind.sql` or the nested `NWFdata` folder to import the schema into your PostgreSQL instance.
   ```bash
   psql -U user -d dbname -f data/northwind.sql
   ```

## Running the Demo

Execute the main script which connects to the database, runs a variety of queries and then shows several matplotlib plots:

```bash
python src/01_main.py
```

The script prints query results to the console and opens interactive chart windows.

## Project Structure

```
README.md
requirements.txt    # Python dependencies
src/
  01_main.py         # demo application
  db/
    connection.py    # async engine/session and stream helpers
    db_utils.py      # QueryRunner with high-level query helpers
data/                # sample Northwind SQL data
```

## Notes & Tips

- `stream()` and `stream_batches()` use a separate `asyncpg` pool and bypass SQLAlchemy. They yield dictionaries for each row or batch.
- Make sure to call `await close_engine()` or `await close_asyncpg_pool()` in long-running applications to cleanly shut down the pool.

## Dependencies

See `requirements.txt` for the full list. Key libraries include:
