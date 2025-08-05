from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Union
from datetime import datetime
from enum import Enum

# Input model for /analyze endpoint
class NotamInput(BaseModel):
    notams: List[str]

# Enums for controlled vocabularies
class SeverityLevel(str, Enum):
    CRITICAL = "CRITICAL"
    OPERATIONAL = "OPERATIONAL"
    ADVISORY = "ADVISORY"

class NotamCategory(str, Enum):
    FIR = "FIR"
    AIRPORT = "Airport"

class UrgencyIndicator(str, Enum):
    IMMEDIATE = "IMMEDIATE"
    URGENT = "URGENT"
    ROUTINE = "ROUTINE"
    PLANNED = "PLANNED"

class FlightPhase(str, Enum):
    PREFLIGHT = "PREFLIGHT"
    TAXI = "TAXI"
    TAKEOFF = "TAKEOFF"
    DEPARTURE = "DEPARTURE"
    CRUISE = "CRUISE"
    APPROACH = "APPROACH"
    LANDING = "LANDING"
    GROUND_OPS = "GROUND_OPS"
    ALL_PHASES = "ALL_PHASES"

class AircraftCategory(str, Enum):
    ALL_AIRCRAFT = "ALL_AIRCRAFT"
    COMMERCIAL = "COMMERCIAL"
    GENERAL_AVIATION = "GENERAL_AVIATION"
    HELICOPTER = "HELICOPTER"
    MILITARY = "MILITARY"
    LARGE_AIRCRAFT = "LARGE_AIRCRAFT"
    SMALL_AIRCRAFT = "SMALL_AIRCRAFT"
    SPECIAL_OPS = "SPECIAL_OPS"

class TimeClassification(str, Enum):
    PERMANENT = "PERMANENT"
    LONG_TERM = "LONG_TERM"
    MEDIUM_TERM = "MEDIUM_TERM"
    SHORT_TERM = "SHORT_TERM"
    DAILY = "DAILY"
    EVENT_SPECIFIC = "EVENT_SPECIFIC"

class AppliedScenario(str, Enum):
    DEPARTURE = "Departure"
    ARRIVAL = "Arrival"
    BOTH = "Both"
    GROUND_ONLY = "Ground_Only"
    ENROUTE = "Enroute"

# Sub-models for complex structures
class Coordinate(BaseModel):
    latitude: float = Field(description="Latitude in decimal degrees")
    longitude: float = Field(description="Longitude in decimal degrees")
    elevation_ft: Optional[int] = Field(None, description="Elevation in feet above MSL")
    reference: Optional[str] = Field(None, description="Reference point description")

class AffectedArea(BaseModel):
    center: Coordinate = Field(description="Center point of affected area")
    radius_nm: Optional[float] = Field(None, description="Radius in nautical miles")
    altitude_lower_ft: Optional[int] = Field(None, description="Lower altitude limit in feet")
    altitude_upper_ft: Optional[int] = Field(None, description="Upper altitude limit in feet")
    shape: Optional[str] = Field(None, description="Area shape: CIRCLE, POLYGON, CORRIDOR")
    vertices: Optional[List[Coordinate]] = Field(None, description="Polygon vertices if applicable")

class ExtractedFrequency(BaseModel):
    frequency: str = Field(description="Frequency value (e.g., '123.45 MHz')")
    type: str = Field(description="Frequency type: TOWER, GROUND, APPROACH, ATIS, NAV")
    previous: Optional[str] = Field(None, description="Previous frequency if changed")

class ExtractedNavaid(BaseModel):
    identifier: str = Field(description="Navaid identifier (e.g., 'SEA')")
    type: str = Field(description="Navaid type: ILS, VOR, DME, NDB, etc.")
    status: str = Field(description="Status: U/S, DEGRADED, LIMITED, TESTING")
    frequency: Optional[str] = Field(None, description="Navaid frequency if applicable")

class ExtractedObstacle(BaseModel):
    type: str = Field(description="Obstacle type: CRANE, TOWER, BALLOON, etc.")
    height_agl_ft: int = Field(description="Height above ground level in feet")
    height_amsl_ft: Optional[int] = Field(None, description="Height above mean sea level in feet")
    location: Coordinate = Field(description="Obstacle location")
    lighting: str = Field(description="Lighting status: LIT, UNLIT, PARTIAL")
    marking: Optional[str] = Field(None, description="Marking description if applicable")

class RunwayCondition(BaseModel):
    runway_id: str = Field(description="Runway identifier (e.g., '09L/27R')")
    condition: Optional[str] = Field(None, description="Surface condition if applicable")
    friction_value: Optional[str] = Field(None, description="Friction measurement if available")
    contaminant_type: Optional[str] = Field(None, description="Type of contaminant")
    contaminant_depth_mm: Optional[int] = Field(None, description="Contaminant depth in millimeters")

class ExtractedElements(BaseModel):
    frequencies: List[ExtractedFrequency] = Field(default_factory=list)
    runways: List[str] = Field(default_factory=list, description="Affected runway identifiers")
    runway_conditions: List[RunwayCondition] = Field(default_factory=list)
    taxiways: List[str] = Field(default_factory=list, description="Affected taxiway identifiers")
    navaids: List[ExtractedNavaid] = Field(default_factory=list)
    obstacles: List[ExtractedObstacle] = Field(default_factory=list)
    procedures: List[str] = Field(default_factory=list, description="Affected procedure names")
    altitudes: List[int] = Field(default_factory=list, description="Extracted altitude values in feet")

class OperationalImpact(BaseModel):
    primary_impact: str = Field(description="Main operational impact description")
    capacity_reduction_percent: Optional[int] = Field(None, description="Airport capacity reduction percentage")
    alternate_required: bool = Field(default=False, description="Whether alternate airport planning is required")
    fuel_impact: Optional[str] = Field(None, description="Impact on fuel planning")
    ground_stop_possible: bool = Field(default=False, description="Whether ground stops are possible")
    delay_expected_min: Optional[int] = Field(None, description="Expected delay in minutes")

class SafetyAssessment(BaseModel):
    safety_critical: bool = Field(description="Whether this NOTAM is safety critical")
    emergency_impact: bool = Field(description="Whether emergency services are affected")
    risk_level: str = Field(description="Risk assessment: LOW, MEDIUM, HIGH, SEVERE")
    mitigations_available: Optional[List[str]] = Field(None, description="Available mitigation measures")


# Main NOTAM Analysis Model
class Notam_Analysis(BaseModel):
    # Basic Information (maintaining compatibility)
    notam_number: str = Field(description="Unique identifier for the NOTAM")
    issue_time: str = Field(description="Date and time the NOTAM was issued, in ISO 8601 UTC format")
    notam_info_type: str = Field(description="Q-code (5-letter NOTAM code indicating type)")
    notam_category: NotamCategory = Field(description="NOTAM scope: 'FIR' or 'Airport'")

    # Enhanced Severity Classification
    seriousness: int = Field(description="Legacy severity level: 1 (Low), 2 (Medium), or 3 (High)")
    severity_level: SeverityLevel = Field(description="Enhanced severity classification")
    urgency_indicator: UrgencyIndicator = Field(description="Urgency classification")

    # Temporal Information
    start_time: str = Field(description="Start time in ISO 8601 UTC format (B field)")
    end_time: Optional[str] = Field(None, description="End time in ISO 8601 UTC format (C field), null if permanent")
    time_classification: TimeClassification = Field(description="Duration-based classification")
    schedule: Optional[str] = Field(None, description="Daily/weekly schedule if applicable")

    # Applicability (enhanced)
    applied_scenario: AppliedScenario = Field(description="Applicable flight scenario")
    applied_aircraft_type: str = Field(description="Legacy: 'Fixed Wing', 'Helicopter', or 'Both'")
    aircraft_categories: List[AircraftCategory] = Field(description="Detailed aircraft category applicability")
    flight_phases: List[FlightPhase] = Field(description="Affected flight phases")

    # Categorization
    operational_tag: List[str] = Field(description="List of operational tags from predefined pool")
    primary_category: str = Field(description="Primary domain category with subcategory")
    secondary_categories: List[str] = Field(default_factory=list, description="Additional applicable categories")

    # Location Information
    affected_airports: List[str] = Field(default_factory=list, description="List of affected airport ICAO codes")
    affected_fir: Optional[str] = Field(None, description="Affected FIR identifier")
    affected_coordinate: Optional[str] = Field(None, description="Legacy coordinate string")
    affected_area: Optional[AffectedArea] = Field(None, description="Detailed affected area information")

    # Infrastructure Impact
    affected_runway: List[str] = Field(description="List of affected runways")
    extracted_elements: ExtractedElements = Field(description="All extracted technical elements")

    # Operational Analysis
    notam_summary: str = Field(description="Brief human-readable summary of the NOTAM")
    operational_impact: OperationalImpact = Field(description="Detailed operational impact assessment")
    safety_assessment: SafetyAssessment = Field(description="Safety criticality assessment")

    # Administrative
    replacing_notam: Optional[str] = Field(None, description="NOTAM number this notice replaces")
    replaced_by: Optional[str] = Field(None, description="NOTAM number that replaces this one")
    related_notams: List[str] = Field(default_factory=list, description="Related NOTAM numbers")

    # Multi-category Support
    multi_category_rationale: Optional[str] = Field(None, description="Explanation for multiple category assignment")

    # App-specific Fields
    requires_acknowledgment: bool = Field(default=False, description="Whether pilot acknowledgment is required")
    display_priority: int = Field(description="Display priority score (1-100, higher = more prominent)")
    filter_tags: List[str] = Field(description="All applicable filter tags for app filtering")

    # Validation and Quality
    confidence_score: float = Field(description="AI confidence score (0.0-1.0)")
    validation_warnings: List[str] = Field(default_factory=list, description="Any validation warnings")

    # Raw Data
    raw_text: Optional[str] = Field(None, description="Original NOTAM text for reference")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


# Helper function to calculate display priority
def calculate_display_priority(analysis: Notam_Analysis) -> int:
    """Calculate display priority based on multiple factors"""
    priority = 50  # Base priority

    # Severity impact
    if analysis.severity_level == SeverityLevel.CRITICAL:
        priority += 30
    elif analysis.severity_level == SeverityLevel.OPERATIONAL:
        priority += 15

    # Urgency impact
    if analysis.urgency_indicator == UrgencyIndicator.IMMEDIATE:
        priority += 15
    elif analysis.urgency_indicator == UrgencyIndicator.URGENT:
        priority += 10

    # Safety critical
    if analysis.safety_assessment.safety_critical:
        priority += 10

    # Emergency services impact
    if analysis.safety_assessment.emergency_impact:
        priority += 5

    # Multiple runway impact
    if len(analysis.affected_runway) > 1:
        priority += 5

    return min(priority, 100)  # Cap at 100


class Notam_Briefing(BaseModel):
    summary: str = Field(description="Detailed and personalized briefing of the NOTAM.")

class Notam_Query_User_Input_Parser(BaseModel):
    airport: str = Field(description="Interested Airport code.")
    flight_scenario: str = Field(description="Flight scenario.")