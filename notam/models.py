# models.py
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from notam.core.enums import (
    SeverityLevelEnum as SeverityLevel,
    NotamCategoryEnum as NotamCategory,
    FlightPhaseEnum as FlightPhase,
    TimeOfDayApplicabilityEnum as TimeOfDayApplicability,
    FlightRuleApplicabilityEnum as FlightRuleApplicability,
    AircraftSizeEnum as AircraftSize,
    AircraftPropulsionEnum as AircraftPropulsion,
    PrimaryCategoryEnum as PrimaryCategory,
    OperationalTagEnum as OperationalTag
)

# Sub-models for complex structures
class Coordinate(BaseModel):
    latitude: float = Field(description="Latitude in decimal degrees")
    longitude: float = Field(description="Longitude in decimal degrees")

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

class ExtractedRunwayCondition(BaseModel):
    runway_id: str = Field(description="Runway identifiers must be in the format NN or NNX (e.g., 07, 07L, 25R)")
    friction_value: Optional[int] = Field(None, description="Friction measurement 0-6 if available")

class ExtractedElements(BaseModel):
    runways: List[str] = Field(default_factory=list, description="Affected runway identifiers")
    runway_conditions: List[ExtractedRunwayCondition] = Field(default_factory=list)
    taxiways: List[str] = Field(default_factory=list, description="Store only the Affected taxiway identifiers")
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

class Notam_Briefing(BaseModel):
    summary: str = Field(description="Detailed and personalized briefing of the NOTAM.")

class Notam_Query_User_Input_Parser(BaseModel):
    airport: str = Field(description="Interested Airport code.")
    flight_scenario: str = Field(description="Flight scenario.")