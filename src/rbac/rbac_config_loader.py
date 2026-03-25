import json
from pathlib import Path
from rbac_env_config import UserSecrets


class RBACConfigError(Exception):
    pass


class RBACConfig:
    """
    RBAC configuration loader + validator for split config structure:

    config/
      database.json
      metadata.json
      login_roles.json
      schemas/
        public.json
        billing.json
        analytics.json
    """

    def __init__(self, base_path: Path, secrets: UserSecrets):
        self.base_path = Path(base_path)
        self.secrets = secrets

        if not self.base_path.exists():
            raise RBACConfigError(f"Config path not found: {self.base_path.resolve()}")

        # Load split config files
        self.database = self._load_json("rbac_config_db.json").get("database")
        self.database_privileges = self._load_json("rbac_config_db.json").get("database_privileges", {})

        self.metadata = self._load_json("rbac_config_metadata.json").get("metadata", {})

        self.login_roles = self._load_json("rbac_config_login_roles.json").get("login_roles", {})

        # Load all schema files into one dict
        self.schemas = self._load_all_schemas()

        # Validate structure
        self._validate()

    # ------------------------------------------------------------
    # File loaders
    # ------------------------------------------------------------
    def _load_json(self, filename: str) -> dict:
        file_path = self.base_path / filename
        if not file_path.exists():
            raise RBACConfigError(f"Missing required config file: {filename}")

        try:
            with file_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            raise RBACConfigError(f"Failed to load JSON file {filename}: {e}")

    def _load_all_schemas(self) -> dict:
        schema_dir = self.base_path / "schemas"
        if not schema_dir.exists():
            raise RBACConfigError("Missing 'schemas/' directory inside config path")

        schemas = {}

        for file in schema_dir.glob("*.json"):
            try:
                with file.open("r", encoding="utf-8") as f:
                    schema_cfg = json.load(f)
            except Exception as e:
                raise RBACConfigError(f"Failed to load schema file {file.name}: {e}")

            schema_name = schema_cfg.get("schema")
            if not schema_name:
                raise RBACConfigError(f"Schema file '{file.name}' missing required key 'schema'")

            schemas[schema_name] = schema_cfg

        if not schemas:
            raise RBACConfigError("No schema JSON files found in schemas/ directory")

        return schemas

    # ------------------------------------------------------------
    # Validation logic
    # ------------------------------------------------------------
    def _validate(self):
        # Validate schemas
        if not isinstance(self.schemas, dict):
            raise RBACConfigError("'schemas' must be dict[str, schema_cfg]")

        for schema_name, schema_cfg in self.schemas.items():
            required_fields = [
                "admin_role",
                "rw_role",
                "ro_role",
                "grants",
                "default_privileges",
            ]
            for field in required_fields:
                if field not in schema_cfg:
                    raise RBACConfigError(
                        f"Schema '{schema_name}' missing required field '{field}'"
                    )

        # Validate login roles
        if not isinstance(self.login_roles, dict):
            raise RBACConfigError("'login_roles' must be dict[str, role_cfg]")

        for login, cfg in self.login_roles.items():
            if "password_env" not in cfg:
                raise RBACConfigError(f"Login role '{login}' missing 'password_env'")
            if "granted_roles" not in cfg:
                raise RBACConfigError(f"Login role '{login}' missing 'granted_roles'")

    # ------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------
    def get_login_password(self, login: str) -> str:
        cfg = self.login_roles[login]
        env_var = cfg["password_env"]

        try:
            return self.secrets.get(env_var)
        except KeyError:
            raise RBACConfigError(
                f"Environment variable '{env_var}' not found for login role '{login}'"
            )

    def get_all_login_roles(self):
        return list(self.login_roles.keys())

    def get_schema_roles(self, schema_name: str):
        schema = self.schemas[schema_name]
        return {
            "admin": schema["admin_role"],
            "rw": schema["rw_role"],
            "ro": schema["ro_role"],
        }

    def get_database_connect_roles(self):
        return self.database_privileges.get("grant_connect_to", [])

    def dump(self):
        import pprint
        pprint.pprint({
            "database": self.database,
            "schemas": self.schemas,
            "login_roles": self.login_roles,
            "database_privileges": self.database_privileges,
            "metadata": self.metadata
        })
