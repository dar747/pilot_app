# notam/scoring.py
from datetime import datetime, timezone
from typing import Tuple, Dict, Any

from notam.db import (
    SeverityLevelEnum,
    TimeClassificationEnum,
    FlightRuleApplicabilityEnum,
)

CATEGORY_BONUS = {
    "RUNWAY_OPERATIONS": 15,
    "NAVAIDS_SID_STAR_APPROACH_PROCEDURES": 10,
    "OBSTACLES": 10,
}

def _enum_value(x) -> str:
    return x.value if hasattr(x, "value") else str(x)

def _parse_utc(val: str | None) -> datetime | None:
    """Parse stored ISO8601 string into aware UTC datetime."""
    if not val:
        return None
    s = val.strip()
    if s.upper() in {"NULL", "NONE", ""}:
        return None
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def compute_base_score(n) -> Tuple[int, Dict[str, Any], str]:
    """
    Compute a 0–100 base score + feature dict + short explanation.
    `n` is a NotamRecord instance.
    """
    now = datetime.now(timezone.utc)
    score = 0
    why = []

    # Severity
    sev = _enum_value(n.severity_level)
    if sev == "CRITICAL":
        score += 60; why.append("critical")
    elif sev == "OPERATIONAL":
        score += 35; why.append("operational")
    else:
        score += 10; why.append("advisory")

    # Parse times (since they are strings in DB now)
    start_dt = _parse_utc(n.start_time)
    end_dt   = _parse_utc(n.end_time)

    # Time proximity
    starts_in_hours = None
    if start_dt:
        starts_in_hours = (start_dt - now).total_seconds() / 3600
        if starts_in_hours <= 24:
            score += 20; why.append("starts ≤24h")
        elif starts_in_hours <= 24*7:
            score += 10; why.append("starts ≤7d")

    # Active now
    active_now = False
    if start_dt and (end_dt or n.time_classification == TimeClassificationEnum.PERMANENT):
        end_dt = end_dt or (now.replace(year=now.year + 10))
        active_now = (start_dt <= now <= end_dt)
        if active_now:
            score += 10; why.append("active now")

    # Primary category bonus
    cat = _enum_value(n.primary_category) if n.primary_category else None
    score += CATEGORY_BONUS.get(cat, 0)
    if CATEGORY_BONUS.get(cat, 0):
        why.append(f"category {cat}")

    # IFR
    if n.flight_rule_applicability == FlightRuleApplicabilityEnum.IFR_ONLY:
        score += 5; why.append("IFR")

    # Presence flags
    has_runway = bool(getattr(n, "runways", []))
    has_procedure = bool(getattr(n, "procedures", []))

    features = {
        "severity": sev,
        "starts_in_hours": starts_in_hours,
        "active_now": active_now,
        "primary_category": cat,
        "has_runway": has_runway,
        "has_procedure": has_procedure,
        "ifr_only": n.flight_rule_applicability == FlightRuleApplicabilityEnum.IFR_ONLY,
        "vfr_only": n.flight_rule_applicability == FlightRuleApplicabilityEnum.VFR_ONLY,
    }

    score = max(0, min(100, int(round(score))))
    return score, features, ", ".join(why[:5])
