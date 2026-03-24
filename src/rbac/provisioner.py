from sqlalchemy import create_engine, text
from errors import SQLExecutionError
from masking import SecretMasker


class RBACProvisioner:
    def __init__(self, config, audit_logger):
        self.config = config
        self.audit = audit_logger
        self.strict_mode = config.strict_mode

        # Real DB engine (for provisioning)
        self.engine = create_engine(config.db_admin_url, echo=config.echo, future=True)

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
        sp = conn.begin_nested()  # SAVEPOINT

        try:
            conn.execute(text(sql), params)
            self.audit.log(audit_action, audit_detail or sql)
            sp.commit()  # savepoint succeeded

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
                sp.rollback()  # rollback only the SAVEPOINT
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
        for user, pw in self.config.login_roles.items():
            self.run(
                conn,
                sql=f"CREATE ROLE {user} LOGIN PASSWORD :pw;",
                audit_action=f"Create login role {user}",
                audit_detail=f"CREATE ROLE {user} LOGIN PASSWORD '***MASKED***'",
                pw=pw
            )

    # ------------------------------------------------------------
    # CREATE SCHEMA + ROLE BUNDLES
    # ------------------------------------------------------------
    def create_schema_roles(self, conn):
        for s in self.config.schemas:

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

            # --- GRANTS (you can add your full grant logic here) ---
            # For example:
            #
            # self.run(conn,
            #     f"GRANT USAGE ON SCHEMA {s} TO {s}_rw;",
            #     f"Grant RW usage on {s}"
            # )

            # Extend with your actual GRANT logic as needed.

    # ------------------------------------------------------------
    # ATTACH LOGIN USERS TO ROLES
    # ------------------------------------------------------------
    def attach_logins(self, conn):
        # Example mappings
        self.run(
            conn,
            sql="GRANT public_rw TO user_john;",
            audit_action="Grant public_rw to user_john")

        self.run(
            conn,
            sql="GRANT billing_ro TO user_john;",
            audit_action="Grant billing_ro to user_john")

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
                self.create_login_roles(conn)
                self.create_schema_roles(conn)
                self.attach_logins(conn)

                # engine.begin() auto-commits here
                self.audit.log("COMMIT", "Provisioning completed")

            except Exception as e:
                # engine.begin() auto-rolls back
                self.audit.log("ROLLBACK", self.masker.mask(str(e)))
                raise
