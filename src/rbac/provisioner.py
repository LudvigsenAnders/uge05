from sqlalchemy import create_engine, text
from errors import SQLExecutionError
from masking import SecretMasker


class RBACProvisioner:
    def __init__(self, env_cfg, rbac_cfg, audit_logger):
        self.env_cfg = env_cfg
        self.rbac_cfg = rbac_cfg
        self.audit = audit_logger
        self.strict_mode = env_cfg.strict_mode

        # Real DB engine (for provisioning)
        self.engine = create_engine(env_cfg.db_admin_url, echo=env_cfg.echo, future=True)

        # secret masking
        self.masker = SecretMasker()

    # ------------------------------------------------------------
    # SAFE, IDEMPOTENT SQL EXECUTION (WITH SAVEPOINT)
    # ------------------------------------------------------------
    def run(self, conn, sql, audit_action, audit_detail=None, **params):
        """
        Run SQL with PostgreSQL SAVEPOINT-based idempotency:
        - Duplicate errors (already exists / does not exist) → SKIPPED
        - Real errors → logged + raise SQLExecutionError
        """
        sp = conn.begin_nested()

        try:
            conn.execute(text(sql), params)
            self.audit.log(audit_action, audit_detail or sql)
            sp.commit()

        except Exception as e:
            msg = str(e).lower()

            # errors safe to skip
            idempotent_errors = (
                "already exists",
                "duplicate",
                "duplicate_object",
                "duplicate key",
                "duplicate_table",
                "duplicate_schema",
                "role exists",
                "duplicate database",
                "duplicate function",
                "duplicate extension",
                "duplicate relation",
                "does not exist",
                "undefined table",
                "undefined relation",
                "unknown role",
                "role does not exist",
                "schema does not exist",
                "relation does not exist",
                "object does not exist",
            )

            if any(err in msg for err in idempotent_errors):
                sp.rollback()
                masked = self.masker.mask(str(e))
                self.audit.log(f"SKIPPED: {audit_action}", masked)
                return

            # real failure
            sp.rollback()
            masked = self.masker.mask(str(e))
            self.audit.log(f"SQL ERROR: {audit_action}", masked)
            raise SQLExecutionError(masked)

    # ------------------------------------------------------------
    # REVOKE OPERATIONS (SAFE + IDEMPOTENT)
    # ------------------------------------------------------------
    def revoke_public_on_schema(self, conn, schema):
        self.run(
            conn,
            f"REVOKE ALL ON SCHEMA {schema} FROM PUBLIC;",
            f"Revoke PUBLIC on schema {schema}"
        )

    def revoke_public_on_tables(self, conn, schema):
        self.run(
            conn,
            f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} FROM PUBLIC;",
            f"Revoke PUBLIC table privileges in schema {schema}"
        )

    def revoke_public_on_sequences(self, conn, schema):
        self.run(
            conn,
            f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} FROM PUBLIC;",
            f"Revoke PUBLIC sequence privileges in schema {schema}"
        )

    def revoke_public_on_database(self, conn, database):
        self.run(
            conn,
            f"REVOKE ALL ON DATABASE {database} FROM PUBLIC;",
            f"Revoke PUBLIC privileges on database {database}"
        )

    # ------------------------------------------------------------
    # CREATE LOGIN ROLES
    # ------------------------------------------------------------
    def create_login_roles(self, conn):

        for user, cfg in self.rbac_cfg.login_roles.items():

            # Load password from ENV using the loader
            pw = self.rbac_cfg.get_login_password(user)

            self.run(
                conn,
                sql=f"CREATE ROLE {user} LOGIN PASSWORD :pw;",
                audit_action=f"Create login role {user}",
                audit_detail=f"CREATE ROLE {user} LOGIN PASSWORD '***MASKED***'",
                pw=pw
            )

    # ------------------------------------------------------------
    # REVOKE ALL (SAFE + IDEMPOTENT)
    # ------------------------------------------------------------
    def revoke_all(self, conn) -> None:
        self.revoke_public_on_database(conn, self.rbac_cfg.database)

        for s in self.rbac_cfg.schemas:
            # secure schema FIRST
            self.revoke_public_on_schema(conn, s)
            self.revoke_public_on_tables(conn, s)
            self.revoke_public_on_sequences(conn, s)

    # ------------------------------------------------------------
    # CREATE SCHEMA + ROLE BUNDLES
    # ------------------------------------------------------------
    def create_schema_roles(self, conn):
        """Orchestrates schema creation, role creation, and permission grants."""
        self.revoke_public_on_database(conn, self.rbac_cfg.database)
        self.create_schema_and_roles(conn)
        self.grant_permissions(conn)

    def create_schema_and_roles(self, conn):

        for schema in self.rbac_cfg.schemas:

            # --- Create schema ---
            self.run(
                conn,
                sql=f"CREATE SCHEMA IF NOT EXISTS {schema};",
                audit_action="Create schema",
                audit_detail=f"Schema: {schema}"
            )

            # --- Create roles: admin / rw / ro ---
            for suffix in ("admin", "rw", "ro"):
                role = f"{schema}_{suffix}"

                create_role_sql = f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                        CREATE ROLE {role};
                    END IF;
                END$$;
                """

                self.run(
                    conn,
                    sql=create_role_sql,
                    audit_action="Create schema role",
                    audit_detail=f"Role: {role}"
                )

    def grant_permissions(self, conn):

        for schema in self.rbac_cfg.schemas:

            # schema-specific RBAC config
            schema_cfg = self.rbac_cfg.schema_rbac[schema]

            role_map = {
                "admin": schema_cfg["admin_role"],
                "rw": schema_cfg["rw_role"],
                "ro": schema_cfg["ro_role"],
            }

            grants = schema_cfg["grants"]
            defaults = schema_cfg["default_privileges"]

            # ---------------------------
            # Standard GRANTS
            # ---------------------------
            for grant_key, role_name in role_map.items():

                rule = grants.get(grant_key, {})

                # schema privileges
                for priv in rule.get("schema", []):
                    self.run(
                        conn,
                        sql=f"GRANT {priv} ON SCHEMA {schema} TO {role_name};",
                        audit_action="Grant schema privilege",
                        audit_detail=f"{priv} on schema {schema} to {role_name}"
                    )

                # table privileges
                for priv in rule.get("tables", []):
                    self.run(
                        conn,
                        sql=f"GRANT {priv} ON ALL TABLES IN SCHEMA {schema} TO {role_name};",
                        audit_action="Grant table privilege",
                        audit_detail=f"{priv} on tables in {schema} to {role_name}"
                    )

                # sequence privileges
                for priv in rule.get("sequences", []):
                    self.run(
                        conn,
                        sql=f"GRANT {priv} ON ALL SEQUENCES IN SCHEMA {schema} TO {role_name};",
                        audit_action="Grant sequence privilege",
                        audit_detail=f"{priv} on sequences in {schema} to {role_name}"
                    )

            # ---------------------------
            # DEFAULT PRIVILEGES
            # ---------------------------
            for grant_key, role_name in role_map.items():

                rule = defaults.get(grant_key, {})

                # default table privileges
                for priv in rule.get("tables", []):
                    self.run(
                        conn,
                        sql=(
                            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} "
                            f"GRANT {priv} ON TABLES TO {role_name};"
                        ),
                        audit_action="Grant default table privilege",
                        audit_detail=f"{priv} on new tables in {schema} to {role_name}"
                    )

                # default sequence privileges
                for priv in rule.get("sequences", []):
                    self.run(
                        conn,
                        sql=(
                            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} "
                            f"GRANT {priv} ON SEQUENCES TO {role_name};"
                        ),
                        audit_action="Grant default sequence privilege",
                        audit_detail=f"{priv} on new sequences in {schema} to {role_name}"
                    )

    # ------------------------------------------------------------
    # ATTACH LOGIN USERS TO ROLES
    # ------------------------------------------------------------
    def attach_logins(self, conn):

        for login_name, cfg in self.rbac_cfg.login_roles.items():
            granted_roles = cfg["granted_roles"]
            for g_role in granted_roles:
                self.run(
                    conn,
                    sql=f"GRANT {g_role} TO {login_name};",
                    audit_action=f"Grant {g_role} to {login_name}"
                )

    # ------------------------------------------------------------
    # SECURE DATABASE
    # ------------------------------------------------------------
    def grant_connection_to_database(self, conn):
        """
        Secure the database by:
        - Revoking PUBLIC access
        - Granting explicit CONNECT to only approved login roles
        """

        database = self.env_cfg.database_name  # your DB name from config

        # 1. Remove unsafe default access
        self.run(
            conn,
            sql=f"REVOKE ALL ON DATABASE {database} FROM PUBLIC;",
            audit_action=f"Revoke PUBLIC privileges on database {database}"
        )

        # 2. Grant CONNECT only to approved login roles
        for login_role in self.rbac_cfg.login_roles.keys():
            self.run(
                conn,
                sql=f"GRANT CONNECT ON DATABASE {database} TO {login_role};",
                audit_action=f"Grant CONNECT on database {database} to {login_role}"
            )

    # ------------------------------------------------------------
    # MAIN PROVISIONING ENTRYPOINT
    # ------------------------------------------------------------
    def provision_all(self):
        """
        Safe provisioning sequence:
        - create login roles
        - create schema roles
        - attach user roles
        - log commit or rollback
        """
        with self.engine.begin() as conn:
            try:
                self.revoke_all(conn)
                self.create_login_roles(conn)
                self.grant_connection_to_database(conn)
                self.create_schema_roles(conn)
                self.attach_logins(conn)

                # engine.begin() auto-commits here
                self.audit.log("COMMIT", "Provisioning completed")

            except Exception as e:
                # engine.begin() auto-rolls back
                self.audit.log("ROLLBACK", self.masker.mask(str(e)))
                raise

    def drop_role(self, role: str, new_owner: str):
        """
        Safely drop a PostgreSQL role:
        1. Reassign all owned objects to new_owner
        2. Drop all privileges granted to role
        3. Drop the role itself
        """
        with self.engine.begin() as conn:
            try:
                # 1. Reassign ownership
                self.run(
                    conn,
                    sql=f"REASSIGN OWNED BY {role} TO {new_owner};",
                    audit_action="Reassign owned objects",
                    audit_detail=f"{role} → {new_owner}"
                )

                # 2. Drop all privileges
                self.run(
                    conn,
                    sql=f"DROP OWNED BY {role};",
                    audit_action="Drop privileges",
                    audit_detail=f"Remove all privileges from {role}"
                )

                # 3. Drop the role itself
                self.run(
                    conn,
                    sql=f"DROP ROLE IF EXISTS {role};",
                    audit_action="Drop role",
                    audit_detail=f"Role: {role}"
                )

            except Exception as e:
                # engine.begin() auto-rolls back
                self.audit.log("ROLLBACK", self.masker.mask(str(e)))
                raise

    def drop_login_role(self, login_role: str, new_owner: str):
        """
        Safely drop a PostgreSQL login role.
        Steps:
        1. Reassign owned objects to new_owner
        2. Drop all privileges the login role has
        3. Remove membership from other roles
        4. Drop the login role
        """
        with self.engine.begin() as conn:
            try:
                # 1. Reassign ownership
                self.run(
                    conn,
                    sql=f"REASSIGN OWNED BY {login_role} TO {new_owner};",
                    audit_action="Reassign owned objects (login role)",
                    audit_detail=f"{login_role} → {new_owner}"
                )

                # 2. Drop privileges
                self.run(
                    conn,
                    sql=f"DROP OWNED BY {login_role};",
                    audit_action="Drop privileges (login role)",
                    audit_detail=f"Drop privileges for {login_role}"
                )

                # 3. Revoke role memberships
                self.run(
                    conn,
                    sql=f"REVOKE {login_role} FROM PUBLIC;",
                    audit_action="Revoke PUBLIC membership (login role)",
                    audit_detail=f"Remove {login_role} from PUBLIC"
                )

                # 4. Drop the role
                self.run(
                    conn,
                    sql=f"DROP ROLE IF EXISTS {login_role};",
                    audit_action="Drop login role",
                    audit_detail=f"Dropped login role: {login_role}"
                )

            except Exception as e:
                # engine.begin() auto-rolls back
                self.audit.log("ROLLBACK", self.masker.mask(str(e)))
                raise
