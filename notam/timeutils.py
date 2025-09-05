# notam/timeutils.py
from datetime import datetime, timezone
import re

# strip control chars (NUL..US, DEL, zero-width joiners)
_CONTROL_RE = re.compile(r"[\x00-\x1F\x7F\u200B\u200C\u200D]")

# strings we should treat as "no time"
_NULL_TOKENS = {
    "", "NULL", "NONE", "NIL", "N/A", "NA",
    "PERM", "PERMANENT", "UFN", "UNTIL FURTHER NOTICE", "TIL FURTHER NOTICE"
}

def parse_iso_to_utc(dt_like) -> datetime | None:
    """
    Tolerant parser:
      - returns None for null-like tokens or malformed strings
      - accepts '...Z' or timezone offsets
      - assumes naive -> UTC
    """
    if dt_like is None:
        return None

    if isinstance(dt_like, datetime):
        dt = dt_like
    elif isinstance(dt_like, str):
        s = _CONTROL_RE.sub("", dt_like).strip()
        if s.upper() in _NULL_TOKENS:
            return None
        if s.endswith(("Z", "z")):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)  # handles offsets like +08:00
        except Exception:
            # malformed -> just treat as missing
            return None
    else:
        # unsupported type -> missing
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def to_z(dt: datetime | None) -> str | None:
    """Render UTC as 'YYYY-MM-DDTHH:MM:SSZ' for JSON/logs."""
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
