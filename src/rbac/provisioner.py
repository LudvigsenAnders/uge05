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

        self.revoke_public_on_database(conn, self.rbac_cfg.database)

        for s in self.rbac_cfg.schemas:

            # secure schema FIRST
            self.revoke_public_on_schema(conn, s)
            self.revoke_public_on_tables(conn, s)
            self.revoke_public_on_sequences(conn, s)

            self.run(
                conn,
                sql=f"CREATE SCHEMA IF NOT EXISTS {s};",
                audit_action="Create schema",
                audit_detail=f"Schema: {s}"
            )

            for suffix in ("admin", "rw", "ro"):
                role = f"{s}_{suffix}"
                self.run(
                    conn,
                    sql=f"CREATE ROLE {role};",
                    audit_action="Create schema role",
                    audit_detail=f"Role: {role}"
                )

            # --- GRANTS (add the full grant logic here) ---
            # For example:
            #
            # self.run(conn,
            #     f"GRANT USAGE ON SCHEMA {s} TO {s}_rw;",
            #     f"Grant RW usage on {s}"
            # )

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

        # # Example mappings
        # self.run(
        #     conn,
        #     sql="GRANT public_rw TO user_john;",
        #     audit_action="Grant public_rw to user_john")

        # # database level
        # sql_revoke_db_lvl="REVOKE ALL ON DATABASE mydb FROM PUBLIC;"
        # sql_grant_db_lvl="GRANT CONNECT ON DATABASE mydb TO <allowed users>;"
        # # schema level
        # sql_revoke_schema_lvl="REVOKE ALL ON SCHEMA billing FROM PUBLIC;"
        # sql_grant_schema_lvl="GRANT USAGE ON SCHEMA billing TO billing_rw, billing_ro;"
        # # table level
        # sql_grant_table_lvl="GRANT SELECT ON ALL TABLES IN SCHEMA billing TO billing_ro;"
        # # alter default privileges
        # sql_alter="ALTER DEFAULT PRIVILEGES"

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
