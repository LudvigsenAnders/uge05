# Make sure:
# PostgreSQL superuser credentials are correct in config.py
# Target schemas are listed in SCHEMAS
# Login users are listed in LOGIN_ROLES

from dotenv import load_dotenv
import os

load_dotenv()


class Config:
    def __init__(self):
        self.schemas = ["nw_public", "nw_billing", "nw_analytics"]

        self.login_roles = {
            "nw_user_john": os.getenv("USER_JOHN_PASSWORD", "changeme1"),
            "nw_user_service": os.getenv("USER_SERVICE_PASSWORD", "changeme2"),
        }

        self.database_name = os.getenv("POSTGRES_DB", "mydb")

        self.db_admin_url = (
            f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'postgres')}:"
            f"{os.getenv('POSTGRES_PASSWORD', 'postgres')}@"
            f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
            f"{os.getenv('POSTGRES_PORT', '5432')}/"
            f"{self.database_name}"
        )

        self.audit_log_file = os.getenv("AUDIT_LOG_FILE", "audit.log")
        self.strict_mode = os.getenv("STRICT_MODE", "true").lower() == "true"
        self.echo = os.getenv("SQL_ECHO", "false").lower() == "true"
