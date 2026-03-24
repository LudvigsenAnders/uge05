import re
import json
from typing import Any, Dict


class SecretMasker:
    """
    A full-featured sensitive-data masking utility.
    Covers:
    - SQL-style secrets    (password='x', token="y")
    - Key-value secrets    (password: x)
    - JSON-style secrets   {"password": "x"}
    - Dict-style params    {'pw': 'x'}
    - SQLAlchemy params    {"pw": "x"}
    """

    # All the sensitive keys we want to detect
    SENSITIVE_KEYS = [
        "password", "passwd", "pwd", "pw",
        "secret", "token", "api_key", "api-key",
        "auth_token", "auth-token", "credential", "key"
    ]

    # Regex covering single, double and unquoted values
    # e.g. password='x', password="x", password=x
    VALUE_PATTERNS = [
        # single-quoted
        r"(?i)(password|passwd|pwd|pw|secret|token|api[_-]?key|auth[_-]?token)\s*[:=]\s*'[^']+'",
        # double-quoted
        r"(?i)(password|passwd|pwd|pw|secret|token|api[_-]?key|auth[_-]?token)\s*[:=]\s*\"[^\"]+\"",
        # unquoted: password=abc123
        r"(?i)(password|passwd|pwd|pw|secret|token|api[_-]?key|auth[_-]?token)\s*[:=]\s*[^\s]+",
        # JSON:  "password": "xyz"
        r"(?i)\"(password|passwd|pwd|pw|secret|token|api[_-]?key|auth[_-]?token)\"\s*:\s*\"[^\"]+\"",
        # YAML-like: password: xyz
        r"(?i)(password|passwd|pwd|pw|secret|token|api[_-]?key|auth[_-]?token)\s*:\s*[^\s]+",
    ]

    MASK = "***MASKED***"

    def __init__(self):
        # Precompile regex patterns
        self.compiled_patterns = [re.compile(p) for p in self.VALUE_PATTERNS]

    # ------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------

    def mask(self, text: str) -> str:
        """
        Mask sensitive values in a string (SQL, logs, JSON, etc.)
        """
        if not text:
            return text

        masked = text

        # 1. Mask textual key-value style secrets
        for regex in self.compiled_patterns:
            masked = regex.sub(self._mask_value_match, masked)

        # 2. Mask dict-like parameter dumps
        masked = self._mask_dict_like(masked)

        return masked

    # ------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------

    def _mask_value_match(self, match: re.Match) -> str:
        """
        Replace the secret value with MASK while preserving key and quoting.
        """
        full = match.group(0)

        # Replace everything after the = or : with the masked version
        return re.sub(
            r"([:=]\s*)(['\"]?)[^'\"]+(['\"]?)",
            r"\1\2" + self.MASK + r"\3",
            full
        )

    def _mask_dict_like(self, text: str) -> str:
        """
        Detect and mask secrets inside dict-like substrings,
        e.g. {'pw': 'secret', 'foo': 'bar'}
        """
        dict_pattern = r"\{[^{}]*\}"
        matches = re.findall(dict_pattern, text)

        for m in matches:
            try:
                # Convert to JSON-compatible by replacing single quotes → double quotes
                j = m.replace("'", '"')
                data = json.loads(j)

                if isinstance(data, dict):
                    masked_dict = self._mask_dict(data)

                    # Convert back to JSON to replace original safely
                    new = json.dumps(masked_dict)
                    text = text.replace(m, new)
            except Exception:
                # Not a dict in practice → ignore
                pass

        return text

    def _mask_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mask values for sensitive keys inside Python/JSON-style dictionaries.
        """
        masked = {}
        for key, val in data.items():
            if key.lower() in self.SENSITIVE_KEYS:
                masked[key] = self.MASK
            else:
                masked[key] = val
        return masked
