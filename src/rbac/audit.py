import json
import datetime
from sqlalchemy import text, create_engine
from masking import SecretMasker
from errors import AuditLoggingError


class AuditLogger:
    def __init__(self, env_cfg, audit_file):
        self.masker = SecretMasker()
        self.audit_engine = create_engine(env_cfg.db_admin_url, future=True)
        self.audit_file = audit_file

    def log(self, action, detail=None):
        # Ensure table exists BEFORE writing logs
        self.ensure_table()

        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        action_masked = self.masker.mask(action)
        detail_masked = self.masker.mask(detail)

        entry = {
            "timestamp": timestamp,
            "action": action_masked,
            "detail": detail_masked,
        }

        # Write to file
        try:
            with open(self.audit_file, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            raise AuditLoggingError(f"File audit failed: {e}")

        # Write to DB
        try:
            with self.audit_engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO security_audit (action, detail)
                        VALUES (:action, :detail)
                    """),
                    {"action": entry["action"], "detail": entry["detail"]},
                )
        except Exception as e:
            raise AuditLoggingError(f"DB audit failed: {e}")

    def ensure_table(self):
        try:
            with self.audit_engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS security_audit (
                        id BIGSERIAL PRIMARY KEY,
                        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        action TEXT NOT NULL,
                        detail TEXT,
                        actor TEXT NOT NULL DEFAULT current_user
                    );
                """))
        except Exception as e:
            raise AuditLoggingError(f"Failed to ensure audit table exists: {e}")
