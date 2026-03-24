import json
import datetime
from sqlalchemy import text
from .masking import mask_sensitive
from .errors import AuditLoggingError


class AuditLogger:
    def __init__(self, audit_engine, audit_file):
        self.audit_engine = audit_engine
        self.audit_file = audit_file

    def log(self, action: str, detail: str = None):
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        entry = {
            "timestamp": timestamp,
            "action": mask_sensitive(action),
            "detail": mask_sensitive(detail),
        }

        # Write to file
        try:
            with open(self.audit_file, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            raise AuditLoggingError(f"File audit failed: {e}")

        # Write to DB (outside provisioning transaction)
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
