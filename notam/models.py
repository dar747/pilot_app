from pydantic import BaseModel, Field
from typing import List


# Input model for /analyze endpoint
class NotamInput(BaseModel):
    notams: List[str]


# Output model returned by LangChain and API
class Notam_Analysis(BaseModel):
    notam_number: str = Field(description="Unique identifier for the NOTAM.")
    issue_time: str = Field(description="Date and time the NOTAM was issued, in ISO 8601 UTC format.")
    notam_info_type: str = Field(description="Q-code (5-letter NOTAM code indicating type).")
    notam_category: str = Field(description="NOTAM scope: 'FIR' or 'Airport'.")
    start_time: str = Field(description="Start time in ISO 8601 UTC format, corresponding to the 'B' field in the NOTAM.")
    end_time: str = Field(description="End time in ISO 8601 UTC format, corresponding to the 'C' field in the NOTAM.")
    seriousness: int = Field(description="Severity level: 1 (Low), 2 (Medium), or 3 (High).")
    applied_scenario: str = Field(description="Applicable scenario: 'Departure', 'Arrival', or 'Both'.")
    applied_aircraft_type: str = Field(description="Applicable aircraft type: 'Fixed Wing', 'Helicopter', or 'Both'.")
    operational_tag: List[str] = Field(description="List of operational tags from a predefined pool.")
    affected_runway: List[str] = Field(description="List of affected runways separated by comma in format, or 'None' if no runway involved.")
    notam_summary: str = Field(description="Brief human-readable summary of the NOTAM.")
    replacing_notam: str = Field(description="NOTAM number this notice replaces, or 'None' if not applicable.")

class Notam_Briefing(BaseModel):
    summary: str = Field(description="Detailed and personalized briefing of the NOTAM.")

class Notam_Query_User_Input_Parser(BaseModel):
    airport: str = Field(description="Interested Airport code.")
    flight_scenario: str = Field(description="Flight scenario.")