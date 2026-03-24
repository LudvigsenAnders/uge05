import re

SENSITIVE_PATTERNS = [
    r"password\s*=\s*'[^']+'",
    r"PASSWORD\s+'[^']+'",
    r"token\s*=\s*'[^']+'",
    r"secret\s*=\s*'[^']+'",
    r"(?i)pwd\s*=\s*'[^']+'",
    r"(?i)password\s*'[^']+'",
]


def mask_sensitive(text: str) -> str:
    if not text:
        return text

    masked = text
    for pattern in SENSITIVE_PATTERNS:
        masked = re.sub(pattern, lambda m: f"{m.group(0).split('=')[0]}='***MASKED***'", masked)

    masked = re.sub(
        r"PASSWORD\s+'[^']+'",
        "PASSWORD '***MASKED***'",
        masked,
        flags=re.IGNORECASE,
    )

    return masked
