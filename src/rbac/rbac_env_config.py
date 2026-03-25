from dotenv import load_dotenv
import os

load_dotenv(".env")


class EnvConfig:
    def __init__(self):

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


load_dotenv(".env.users")


class UserSecrets:
    def __init__(self):
        # Load all env vars
        self.users = {
            key: value
            for key, value in os.environ.items()
        }

        if not self.users:
            raise ValueError("No user secrets found in .env.users")

    def get(self, env_name: str):
        if env_name not in self.users:
            raise KeyError(f"User secret '{env_name}' not found")
        return self.users[env_name]
