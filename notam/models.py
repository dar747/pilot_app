from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from enum import Enum


# Enums for controlled vocabularies
class SeverityLevel(str, Enum):
    CRITICAL = "CRITICAL"
    OPERATIONAL = "OPERATIONAL"
    ADVISORY = "ADVISORY"

class NotamCategory(str, Enum):
    FIR = "FIR"
    AIRPORT = "AIRPORT"

class FlightPhase(str, Enum):
    PREFLIGHT = "PREFLIGHT"
    TAXI = "TAXI"
    TAKEOFF = "TAKEOFF"
    DEPARTURE = "DEPARTURE"
    CRUISE = "CRUISE"
    APPROACH = "APPROACH"
    ARRIVAL = "ARRIVAL"
    GROUND_OPS = "GROUND_OPS"
    ALL_PHASES = "ALL_PHASES"

class TimeClassification(str, Enum):
    PERMANENT = "PERMANENT"
    LONG_TERM = "LONG_TERM"
    MEDIUM_TERM = "MEDIUM_TERM"
    SHORT_TERM = "SHORT_TERM"
    DAILY = "DAILY"
    WEEKLY ="WEEKLY"
    MONTHLY = "MONTHLY"
    EVENT_SPECIFIC = "EVENT_SPECIFIC"


class TimeOfDayApplicability(str, Enum):
    DAY = "DAY ONLY"
    NIGHT = "NIGHT ONLY"
    ALL = "ALL TIMES"

class FlightRuleApplicability(str, Enum):
    VFR_ONLY = "VFR ONLY"
    IFR_ONLY = "IFR ONLY"
    ALL = "ALL RULES"

class AircraftSize(str, Enum):
    ALL = "ALL"
    LIGHT = "LIGHT"
    MEDIUM = "MEDIUM"
    HEAVY = "HEAVY"
    SUPER = "SUPER"  # optional

class AircraftPropulsion(str, Enum):
    ALL = "ALL"
    JET = "JET"
    TURBOPROP = "TURBOPROP"
    PISTON = "PISTON"
    HELICOPTER = "HELICOPTER"

class PrimaryCategory(str, Enum):
    RUNWAY_OPERATIONS = "RUNWAY_OPERATIONS"
    AERODROME_OPERATIONS = "AERODROME_OPERATIONS"
    NAVAIDS_SID_STAR_APPROACH_PROCEDURES = "NAVAIDS_SID_STAR_APPROACH_PROCEDURES"
    FLOW_CONTROL = "FLOW_CONTROL"
    COMMUNICATION_SERVICES = "COMMUNICATION_SERVICES"
    OBSTACLES = "OBSTACLES"

class OperationalTag(str, Enum):
    AIRPORT_CLOSURE = "AIRPORT_CLOSURE"
    RUNWAY_CLOSURE = "RUNWAY_CLOSURE"
    RUNWAY_LENGTH_REDUCTION = "RUNWAY_LENGTH_REDUCTION"
    DISPLACED_RUNWAY_THRESHOLD = "DISPLACED_RUNWAY_THRESHOLD"
    RUNWAY_SURFACE_CONTAMINATION = "RUNWAY_SURFACE_CONTAMINATION"
    BRAKING_ACTION_REPORT = "BRAKING_ACTION_REPORT"
    RUNWAY_SURFACE_CONDITION_REPORT = "RUNWAY_SURFACE_CONDITION_REPORT"
    TAXIWAY_LIGHT_UNSERVICEABLE = "TAXIWAY_LIGHT_UNSERVICEABLE"
    RUNWAY_LIGHTS_UNSERVICEABLE = "RUNWAY_LIGHTS_UNSERVICEABLE"
    APPROACH_LIGHTS_UNSERVICEABLE = "APPROACH_LIGHTS_UNSERVICEABLE"
    PAPI_VASI_UNSERVICEABLE = "PAPI_VASI_UNSERVICEABLE"
    TAXIWAY_CLOSURE = "TAXIWAY_CLOSURE"
    TAXIWAY_RESTRICTION = "TAXIWAY_RESTRICTION"
    WINGSPAN_RESTRICTION = "WINGSPAN_RESTRICTION"
    RUNUP_APRON_CLOSURE = "RUNUP_APRON_CLOSURE"
    RAMP_CLOSURE = "RAMP_CLOSURE"
    GATE_RESTRICTION = "GATE_RESTRICTION"
    AIRSPACE_RESTRICTIONS = "AIRSPACE_RESTRICTIONS"
    PROHIBITED_AREA_ACTIVE = "PROHIBITED_AREA_ACTIVE"
    RESTRICTED_DANGER_AREA_ACTIVE = "RESTRICTED_DANGER_AREA_ACTIVE"
    MILITARY_ACTIVITY_ACTIVE = "MILITARY_ACTIVITY_ACTIVE"
    NAVIGATION_AID_TESTING = "NAVIGATION_AID_TESTING"
    NAVAID_UNSERVICEABLE = "NAVAID_UNSERVICEABLE"
    DEGRADED_NAVAID = "DEGRADED_NAVAID"
    ILS_UNSERVICEABLE = "ILS_UNSERVICEABLE"
    DEGRADED_ILS = "DEGRADED_ILS"
    DME_UNSERVICEABLE = "DME_UNSERVICEABLE"
    DEGRADED_DME = "DEGRADED_DME"
    RNP_RNAV_GNSS_DISRUPTION = "RNP_RNAV_GNSS_DISRUPTION"
    MARKER_LOCATOR_UNSERVICEABLE = "MARKER_LOCATOR_UNSERVICEABLE"
    ADSB_COVERAGE_LIMITATION = "ADSB_COVERAGE_LIMITATION"
    SID_PROCEDURES_CHANGE = "SID_PROCEDURES_CHANGE"
    STAR_PROCEDURES_CHANGE = "STAR_PROCEDURES_CHANGE"
    APPROACH_PROCEDURE_CHANGE = "APPROACH_PROCEDURE_CHANGE"
    DEPARTURE_APPROACH_MINIMA_CHANGE = "DEPARTURE_APPROACH_MINIMA_CHANGE"
    MISSED_APPROACH_PROCEDURE_CHANGE = "MISSED_APPROACH_PROCEDURE_CHANGE"
    RVR_EQUIPMENT_UNSERVICEABLE = "RVR_EQUIPMENT_UNSERVICEABLE"
    INSTRUMENT_APPROACH_UNAVAILABLE = "INSTRUMENT_APPROACH_UNAVAILABLE"
    CAT_II_III_UNAVAILABLE = "CAT_II_III_UNAVAILABLE"
    LOW_VISIBILITY_OPERATIONS_SUSPENDED = "LOW_VISIBILITY_OPERATIONS_SUSPENDED"
    UNLIT_OBSTACLE = "UNLIT_OBSTACLE"
    OBSTACLE_HAZARD = "OBSTACLE_HAZARD"
    WILDLIFE_ACTIVITY = "WILDLIFE_ACTIVITY"
    RUNWAY_TAXIWAY_FOD_HAZARD = "RUNWAY_TAXIWAY_FOD_HAZARD"
    FUEL_UNAVAILABLE = "FUEL_UNAVAILABLE"
    DEICING_UNAVAILABLE = "DEICING_UNAVAILABLE"
    ARFF_INDEX_REDUCED = "ARFF_INDEX_REDUCED"
    FLIGHT_PLANNING_SERVICE_UNAVAILABLE = "FLIGHT_PLANNING_SERVICE_UNAVAILABLE"
    PUBLICATION_AMENDMENT = "PUBLICATION_AMENDMENT"
    COMMS_FREQUENCY_CHANGE = "COMMS_FREQUENCY_CHANGE"
    NAVAID_FREQUENCY_CHANGE = "NAVAID_FREQUENCY_CHANGE"
    CPDLC_UNAVAILABLE = "CPDLC_UNAVAILABLE"
    CLIMB_GRADIENT_CHANGE = "CLIMB_GRADIENT_CHANGE"
    NOISE_ABATEMENT = "NOISE_ABATEMENT"
    ATFM_RESTRICTIONS = "ATFM_RESTRICTIONS"
    LIGHT_SHOW = "LIGHT_SHOW"
    LASER_ACTIVITY = "LASER_ACTIVITY"
    WEATHER_AID_UNSERVICEABLE = "WEATHER_AID_UNSERVICEABLE"
    AOG_SUPPORT_LIMITED = "AOG_SUPPORT_LIMITED"
    SAR_RELATED = "SAR_RELATED"
    CIRCUIT_TRAINING_RESTRICTION = "CIRCUIT_TRAINING_RESTRICTION"
    AERODROME_BEACON_UNSERVICEABLE = "AERODROME_BEACON_UNSERVICEABLE"
    DRONE_ACTIVITY = "DRONE_ACTIVITY"
    RUNWAY_HOLD_POSITION_CHANGE = "RUNWAY_HOLD_POSITION_CHANGE"
    DESIGNATOR_CHANGE = "DESIGNATOR_CHANGE"
    ATS_UNAVAILABLE = "ATS_UNAVAILABLE"
    ENROUTE_ROUTE_CHANGE = "ENROUTE_ROUTE_CHANGE"

# Sub-models for complex structures
class Coordinate(BaseModel):
    latitude: float = Field(description="Latitude in decimal degrees")
    longitude: float = Field(description="Longitude in decimal degrees")

class RunwayReferencePoint(BaseModel):
    runway_id: str = Field(description="Runway identifiers must be in the format NN or NNX (e.g., 07, 07L, 25R)")
    reference_type: str = Field(description="Usually 'Threshold' or 'Midpoint'")
    offset_distance_m: float = Field(description="Distance from the reference point in meters")
    offset_direction: Optional[str] = Field(
        None,
        description="Direction from the reference point: 'Along Runway', 'Left', 'Right', 'Opposite', etc."
    )

    lateral_half_width_m: Optional[float] = Field(
        None,
        description="Half-width from runway centerline in meters (if corridor)"
    )
    corridor_orientation: Optional[str] = Field(
        None,
        description="Orientation of the corridor: 'Parallel', 'Perpendicular', etc."
    )


class AffectedArea(BaseModel):
    center: Optional[Coordinate] = Field(description="Center point of affected area")
    radius_nm: Optional[float] = Field(None, description="Radius in nautical miles")
    altitude_lower_ft: Optional[int] = Field(None, description="Lower altitude limit in feet AMSL")
    altitude_upper_ft: Optional[int] = Field(None, description="Upper altitude limit in feet AMSL")
    shape: Optional[str] = Field(None, description="Area shape: CIRCLE, POLYGON, CORRIDOR")
    vertices: Optional[List[Coordinate]] = Field(None, description="Polygon vertices if applicable")



class ExtractedObstacle(BaseModel):
    type: str = Field(description="Obstacle type: CRANE, TOWER, BALLOON, etc.")
    height_agl_ft: int = Field(description="Height above ground level in feet")
    height_amsl_ft: Optional[int] = Field(None, description="Height above mean sea level in feet")
    location: Optional[Coordinate] = Field(None, description="Absolute location if known")
    lighting: str = Field(description="Lighting status: LIT, UNLIT, PARTIAL")
    runway_reference: Optional[RunwayReferencePoint] = Field(
        None,
        description="Relative runway/ARP reference point"
    )

class ExtractedRunwayCondition(BaseModel):
    runway_id: str = Field(description="Runway identifiers must be in the format NN or NNX (e.g., 07, 07L, 25R)")
    friction_value: Optional[int] = Field(None, description="Friction measurement 0-6 if available")

class ExtractedElements(BaseModel):
    runways: List[str] = Field(default_factory=list, description="Affected runway identifiers")
    runway_conditions: List[ExtractedRunwayCondition] = Field(default_factory=list)
    taxiways: List[str] = Field(default_factory=list, description="Affected taxiway identifiers")
    obstacles: List[ExtractedObstacle] = Field(default_factory=list)
    procedures: List[str] = Field(default_factory=list, description="Affected SID/STAR procedure names")


class WingspanRestriction(BaseModel):
    """Numeric bounds for wingspan in meters."""
    min_m: Optional[float] = Field(
        None, description="Minimum wingspan (meters). Omit if no lower bound."
    )
    min_inclusive: bool = Field(
        True, description="Whether the minimum is inclusive (>=)."
    )
    max_m: Optional[float] = Field(
        None, description="Maximum wingspan (meters). Omit if no upper bound."
    )
    max_inclusive: bool = Field(
        True, description="Whether the maximum is inclusive (<=)."
    )

class AircraftApplicability(BaseModel):
    sizes: List[AircraftSize] = Field(default_factory=list, description = "Based on ICAO wake turbulence category")
    propulsion: Optional[List[AircraftPropulsion]] = Field(default=None)
    wingspan_restriction: Optional[WingspanRestriction] = Field(default=None,description="Wingspan bounds in meters for which the restriction applies.")

class SpecificPeriodUTC(BaseModel):
    start_iso: str = Field(description="Date and time the individiual event in the NOTAM was started, in ISO 8601 UTC format")  # "YYYY-MM-DDThh:mm:ssZ"
    end_iso: str = Field(description="Date and time the individiual event in the NOTAM was ended, in ISO 8601 UTC format")   # "YYYY-MM-DDThh:mm:ssZ"


# Main NOTAM Analysis Model
class Notam_Analysis(BaseModel):
    # Basic Information (maintaining compatibility)
    notam_number: str = Field(description="Unique identifier for the NOTAM")
    issue_time: str = Field(description="Date and time the NOTAM was issued, in ISO 8601 UTC format")
    notam_category: NotamCategory = Field(description="NOTAM scope: 'FIR' or 'Airport'")
    operational_instances: Optional[List[SpecificPeriodUTC]] = Field(
        default_factory=list,
        description="List of absolute UTC windows when the event inside the NOTAM is in effect if any."
    )

    # Enhanced Severity Classification
    severity_level: SeverityLevel = Field(description="Enhanced severity classification")

    # Temporal Information
    start_time: str = Field(description="Start time in ISO 8601 UTC format (B field)")
    end_time: str = Field(None, description="End time in ISO 8601 UTC format (C field), store NULL if permanent NOTAM")
    time_classification: TimeClassification = Field(description="Duration-base"
                                                                "d classification")

    # Applicability (enhanced)
    flight_phases: List[FlightPhase] = Field(description="Affected flight phases")
    time_of_day_applicability: TimeOfDayApplicability = Field(description="Whether the NOTAM is relevant for daytime, nighttime ops, or all times")
    flight_rule_applicability: FlightRuleApplicability = Field(description="Whether the NOTAM applies only to VFR, IFR, or all flight rules")
    aircraft_applicability: AircraftApplicability= Field(description="Detailed aircraft category applicability")

    # Categorization
    operational_tag: List[OperationalTag] = Field(description="List of operational tags from predefined pool")
    primary_category: PrimaryCategory = Field(description="Primary category: Assign one of the main category from the Enum List")

    # Location Information
    affected_airports: List[str] = Field(default_factory=list, description="List of affected airport ICAO/FIR codes")
    affected_area: Optional[AffectedArea] = Field(None, description="Detailed affected area information")

    # Infrastructure Impact
    extracted_elements: Optional[ExtractedElements] = Field(description="All extracted technical elements")

    # Operational Analysis
    notam_summary: str = Field(description="Brief human-readable summary of the NOTAM")
    one_line_description: str = Field(description="One line description of the nature of the NOTAM event")

    # Administrative
    replacing_notam: Optional[str] = Field(None, description="NOTAM number this notice replaces")


    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


# # Helper function to calculate display priority
# def calculate_display_priority(analysis: Notam_Analysis) -> int:
#     """Calculate display priority based on multiple factors"""
#     priority = 50  # Base priority
#
#     # Severity impact
#     if analysis.severity_level == SeverityLevel.CRITICAL:
#         priority += 30
#     elif analysis.severity_level == SeverityLevel.OPERATIONAL:
#         priority += 15
#
#     # Urgency impact
#     if analysis.urgency_indicator == UrgencyIndicator.IMMEDIATE:
#         priority += 15
#     elif analysis.urgency_indicator == UrgencyIndicator.URGENT:
#         priority += 10
#
#     # Safety critical
#     if analysis.safety_assessment.safety_critical:
#         priority += 10
#
#     # Emergency services impact
#     if analysis.safety_assessment.emergency_impact:
#         priority += 5
#
#     # Multiple runway impact
#     if len(analysis.affected_runway) > 1:
#         priority += 5
#
#     return min(priority, 100)  # Cap at 100
#

class Notam_Briefing(BaseModel):
    summary: str = Field(description="Detailed and personalized briefing of the NOTAM.")

class Notam_Query_User_Input_Parser(BaseModel):
    airport: str = Field(description="Interested Airport code.")
    flight_scenario: str = Field(description="Flight scenario.")