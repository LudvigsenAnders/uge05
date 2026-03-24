from sqlalchemy import create_engine, text
from .errors import SQLExecutionError
from .masking import mask_sensitive


class RBACProvisioner:
    def __init__(self, config, audit_logger):
        self.config = config
        self.audit = audit_logger
        self.strict_mode = config.strict_mode

        self.engine = create_engine(config.db_admin_url, echo=config.echo, future=True)

    def run(self, conn, sql, audit_action, audit_detail=None, **params):
        try:
            conn.execute(text(sql), params)
            self.audit.log(audit_action, audit_detail or sql)

        except Exception as e:
            masked = mask_sensitive(str(e))
            self.audit.log(f"SQL ERROR: {audit_action}", masked)
            raise SQLExecutionError(masked)

    def create_audit_table(self, conn):
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS security_audit (
                id BIGSERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                action TEXT NOT NULL,
                detail TEXT,
                actor TEXT NOT NULL DEFAULT current_user
            );
        """))

    def create_login_roles(self, conn):
        for user, pw in self.config.login_roles.items():
            self.run(
                conn,
                f"CREATE ROLE {user} LOGIN PASSWORD :pw;",
                f"Create login role {user}",
                f"CREATE ROLE {user} LOGIN PASSWORD '***MASKED***'",
                pw=pw
            )

    def create_schema_roles(self, conn):
        for s in self.config.schemas:
            self.run(conn, f"CREATE SCHEMA IF NOT EXISTS {s};", "Create schema", f"Schema: {s}")

            for suffix in ("admin", "rw", "ro"):
                role = f"{s}_{suffix}"
                self.run(conn, f"CREATE ROLE {role};", "Create schema role", f"Role: {role}")

            # privilege grants omitted here to shorten example—your full code plugs in here

    def attach_logins(self, conn):
        # Example mappings
        self.run(conn, "GRANT public_rw TO user_john;", "Grant public_rw to user_john")
        self.run(conn, "GRANT billing_ro TO user_john;", "Grant billing_ro to user_john")

    def provision_all(self):
        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                self.create_audit_table(conn)
                self.create_login_roles(conn)
                self.create_schema_roles(conn)
                self.attach_logins(conn)

                trans.commit()
                self.audit.log("COMMIT", "Provisioning completed")
            except Exception as e:
                trans.rollback()
                self.audit.log("ROLLBACK", mask_sensitive(str(e)))
                raise
