# notam/scoring.py
from typing import Tuple, Dict, Any

# 0–100: higher = more operationally severe/urgent

# --- IFR-oriented scoring (your current "new" set) ---
IFR_TAG_SCORES: Dict[str, int] = {
    # Closures / runway performance
    "AIRPORT_CLOSURE": 100,
    "RUNWAY_CLOSURE": 95,
    "RUNWAY_LENGTH_REDUCTION": 85,
    "DISPLACED_RUNWAY_THRESHOLD": 60,
    "RUNWAY_SURFACE_CONTAMINATION": 70,
    "BRAKING_ACTION_REPORT": 70,
    "RUNWAY_SURFACE_CONDITION_REPORT": 70,

    # Lights & visual aids
    "RUNWAY_LIGHTS_UNSERVICEABLE": 78,
    "APPROACH_LIGHTS_UNSERVICEABLE": 78,
    "PAPI_VASI_UNSERVICEABLE": 70,
    "TAXIWAY_LIGHT_UNSERVICEABLE": 60,
    "AERODROME_BEACON_UNSERVICEABLE": 25,

    # Movement areas
    "TAXIWAY_CLOSURE": 65,
    "TAXIWAY_RESTRICTION": 50,
    "RUNUP_APRON_CLOSURE": 50,
    "RAMP_CLOSURE": 60,
    "GATE_RESTRICTION": 45,

    # Airspace / activity
    "AIRSPACE_RESTRICTIONS": 85,
    "PROHIBITED_AREA_ACTIVE": 92,
    "RESTRICTED_DANGER_AREA_ACTIVE": 90,
    "MILITARY_ACTIVITY_ACTIVE": 80,
    "DRONE_ACTIVITY": 60,
    "LASER_ACTIVITY": 60,
    "LIGHT_SHOW": 20,

    # NAV/COM/PROCEDURES
    "NAVIGATION_AID_TESTING": 70,
    "NAVAID_UNSERVICEABLE": 75,
    "DEGRADED_NAVAID": 65,
    "ILS_UNSERVICEABLE": 90,
    "DEGRADED_ILS": 82,
    "DME_UNSERVICEABLE": 75,
    "DEGRADED_DME": 55,
    "RNP_RNAV_GNSS_DISRUPTION": 75,
    "MARKER_LOCATOR_UNSERVICEABLE": 40,
    "ADSB_COVERAGE_LIMITATION": 50,
    "SID_PROCEDURES_CHANGE": 70,
    "STAR_PROCEDURES_CHANGE": 70,
    "APPROACH_PROCEDURE_CHANGE": 80,
    "DEPARTURE_APPROACH_MINIMA_CHANGE": 75,
    "MISSED_APPROACH_PROCEDURE_CHANGE": 75,
    "RVR_EQUIPMENT_UNSERVICEABLE": 75,
    "INSTRUMENT_APPROACH_UNAVAILABLE": 90,
    "CAT_II_III_UNAVAILABLE": 88,
    "LOW_VISIBILITY_OPERATIONS_SUSPENDED": 80,
    "COMMS_FREQUENCY_CHANGE": 45,
    "NAVAID_FREQUENCY_CHANGE": 45,
    "CPDLC_UNAVAILABLE": 55,
    "ATS_UNAVAILABLE": 90,

    # Obstacles / hazards
    "UNLIT_OBSTACLE": 45,
    "OBSTACLE_HAZARD": 45,
    "WILDLIFE_ACTIVITY": 55,
    "RUNWAY_TAXIWAY_FOD_HAZARD": 70,

    # Ops support / services
    "FUEL_UNAVAILABLE": 65,
    "DEICING_UNAVAILABLE": 65,
    "ARFF_INDEX_REDUCED": 65,
    "FLIGHT_PLANNING_SERVICE_UNAVAILABLE": 40,
    "WEATHER_AID_UNSERVICEABLE": 40,
    "AOG_SUPPORT_LIMITED": 15,

    # Publications / admin / noise
    "PUBLICATION_AMENDMENT": 20,
    "NOISE_ABATEMENT": 30,

    # Aircraft applicability / designators
    "WINGSPAN_RESTRICTION": 55,
    "CLIMB_GRADIENT_CHANGE": 60,
    "RUNWAY_HOLD_POSITION_CHANGE": 50,
    "DESIGNATOR_CHANGE": 50,

    # Flow
    "ATFM_RESTRICTIONS": 70,

    # SAR / training
    "SAR_RELATED": 30,
    "CIRCUIT_TRAINING_RESTRICTION": 25,

    # Enroute
    "ENROUTE_ROUTE_CHANGE": 60,
}

# --- VFR-oriented scoring (emphasizes visibility, obstacles, lights, airspace) ---
VFR_TAG_SCORES: Dict[str, int] = {
    # Closures / runway performance (critical for all ops)
    "AIRPORT_CLOSURE": 100,
    "RUNWAY_CLOSURE": 95,
    "RUNWAY_LENGTH_REDUCTION": 85,
    "DISPLACED_RUNWAY_THRESHOLD": 70,
    "RUNWAY_SURFACE_CONTAMINATION": 80,
    "BRAKING_ACTION_REPORT": 65,
    "RUNWAY_SURFACE_CONDITION_REPORT": 65,

    # Lights & visual aids (higher for VFR, esp. night ops)
    "RUNWAY_LIGHTS_UNSERVICEABLE": 85,
    "APPROACH_LIGHTS_UNSERVICEABLE": 80,
    "PAPI_VASI_UNSERVICEABLE": 75,
    "TAXIWAY_LIGHT_UNSERVICEABLE": 70,
    "AERODROME_BEACON_UNSERVICEABLE": 70,

    # Movement areas
    "TAXIWAY_CLOSURE": 70,
    "TAXIWAY_RESTRICTION": 60,
    "RUNUP_APRON_CLOSURE": 55,
    "RAMP_CLOSURE": 60,
    "GATE_RESTRICTION": 50,

    # Airspace / activity (bigger impact for VFR)
    "AIRSPACE_RESTRICTIONS": 90,
    "PROHIBITED_AREA_ACTIVE": 95,
    "RESTRICTED_DANGER_AREA_ACTIVE": 92,
    "MILITARY_ACTIVITY_ACTIVE": 85,
    "DRONE_ACTIVITY": 75,
    "LASER_ACTIVITY": 75,
    "LIGHT_SHOW": 40,

    # NAV/COM/PROCEDURES (lower for VFR)
    "NAVIGATION_AID_TESTING": 30,
    "NAVAID_UNSERVICEABLE": 30,
    "DEGRADED_NAVAID": 25,
    "ILS_UNSERVICEABLE": 20,
    "DEGRADED_ILS": 20,
    "DME_UNSERVICEABLE": 25,
    "DEGRADED_DME": 20,
    "RNP_RNAV_GNSS_DISRUPTION": 25,
    "MARKER_LOCATOR_UNSERVICEABLE": 15,
    "ADSB_COVERAGE_LIMITATION": 30,
    "SID_PROCEDURES_CHANGE": 20,
    "STAR_PROCEDURES_CHANGE": 20,
    "APPROACH_PROCEDURE_CHANGE": 25,
    "DEPARTURE_APPROACH_MINIMA_CHANGE": 25,
    "MISSED_APPROACH_PROCEDURE_CHANGE": 20,
    "RVR_EQUIPMENT_UNSERVICEABLE": 15,
    "INSTRUMENT_APPROACH_UNAVAILABLE": 15,
    "CAT_II_III_UNAVAILABLE": 10,
    "LOW_VISIBILITY_OPERATIONS_SUSPENDED": 20,
    "COMMS_FREQUENCY_CHANGE": 40,
    "NAVAID_FREQUENCY_CHANGE": 40,
    "CPDLC_UNAVAILABLE": 20,
    "ATS_UNAVAILABLE": 70,  # still serious for advisories/clearances

    # Obstacles / hazards (much higher for VFR)
    "UNLIT_OBSTACLE": 85,
    "OBSTACLE_HAZARD": 90,
    "WILDLIFE_ACTIVITY": 65,
    "RUNWAY_TAXIWAY_FOD_HAZARD": 75,

    # Ops support / services
    "FUEL_UNAVAILABLE": 75,
    "DEICING_UNAVAILABLE": 50,
    "ARFF_INDEX_REDUCED": 50,
    "FLIGHT_PLANNING_SERVICE_UNAVAILABLE": 40,
    "WEATHER_AID_UNSERVICEABLE": 50,
    "AOG_SUPPORT_LIMITED": 20,

    # Publications / admin / noise
    "PUBLICATION_AMENDMENT": 20,
    "NOISE_ABATEMENT": 40,

    # Aircraft applicability / designators
    "WINGSPAN_RESTRICTION": 50,
    "CLIMB_GRADIENT_CHANGE": 40,
    "RUNWAY_HOLD_POSITION_CHANGE": 45,
    "DESIGNATOR_CHANGE": 40,

    # Flow
    "ATFM_RESTRICTIONS": 40,

    # SAR / training
    "SAR_RELATED": 35,
    "CIRCUIT_TRAINING_RESTRICTION": 40,

    # Enroute
    "ENROUTE_ROUTE_CHANGE": 45,
}

DEFAULT_TAG_SCORE = 20  # fallback for unrecognized tags

# Backward compatibility: existing code importing TAG_SCORES will get IFR by default.
TAG_SCORES = IFR_TAG_SCORES

_PROFILE_MAP: Dict[str, Dict[str, int]] = {
    "IFR": IFR_TAG_SCORES,
    "VFR": VFR_TAG_SCORES,
}

def _select_scores(profile: str) -> Dict[str, int]:
    """
    Return the scoring table for the requested profile.
    Falls back to IFR if profile is unknown/None.
    """
    if not profile:
        return IFR_TAG_SCORES
    key = profile.strip().upper()
    return _PROFILE_MAP.get(key, IFR_TAG_SCORES)

def compute_base_score(n, profile: str = "IFR") -> Tuple[int, Dict[str, Any], str]:
    """
    Base score derived *only* from operational tags.
    Strategy: choose the highest-scoring tag attached to the NOTAM.

    Args:
        n: object with attribute `operational_tags` (iterable of objects with .tag_name)
        profile: "IFR" or "VFR" (case-insensitive). Defaults to "IFR".
                 Unknown values fall back to IFR.

    Returns:
        (score, features, why)
        score: int in [0, 100]
        features: {"tags": List[str], "chosen_tag": Optional[str], "profile": str}
        why: short string explaining the driver (e.g., "tag RUNWAY_CLOSURE")
    """
    scores = _select_scores(profile)
    tags = [t.tag_name for t in getattr(n, "operational_tags", [])]
    if not tags:
        features = {"tags": [], "chosen_tag": None, "profile": profile}
        return DEFAULT_TAG_SCORE, features, "no tags"

    best_tag = None
    best_score = -1
    for t in tags:
        s = scores.get(t, DEFAULT_TAG_SCORE)
        if s > best_score:
            best_score = s
            best_tag = t

    best_score = max(0, min(100, int(best_score)))
    features = {"tags": tags, "chosen_tag": best_tag, "profile": profile}
    why = f"tag {best_tag}" if best_tag else "no tags"
    return best_score, features, why

# Optional: convenience function if you ever want to score an ad-hoc tag list
def compute_base_score_from_tags(tags, profile: str = "IFR") -> Tuple[int, Dict[str, Any], str]:
    """
    Compute a base score directly from an iterable of tag strings.
    Mirrors compute_base_score() behavior.
    """
    class _Temp:
        def __init__(self, tags):
            self.operational_tags = [type("T", (), {"tag_name": t})() for t in tags]
    return compute_base_score(_Temp(tags), profile=profile)
