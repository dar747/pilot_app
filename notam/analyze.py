# analyze.py

import os
from dotenv import load_dotenv

load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from notam.models import Notam_Analysis

llm = ChatOpenAI(model="gpt-4.1-mini", temperature=0, api_key=openai_api_key)

# System instruction with classification guidance
notam_analysis_system_msg = """
You are an expert in categorizing NOTAMs based on seriousness and extracting structured data.

Level 1: Advisory - Information only (Flight planning, publications, political, VFR)
Level 2: Operational - Flight planning required (apron closed, fuel issues, ATIS U/S, SID/STAR changes, UAS/drone activity)
Level 3: Critical - Potentially Safety impact (eg: Runway/ILS/NAV U/S, GNSS outage, minima change, PAPI U/S, airport closure)

Use only these tags:

# NOTAM Allowable Tags - Complete List

## Severity Tags
- `CRITICAL`
- `OPERATIONAL` 
- `ADVISORY`

## Aerodrome Operations Tags
- `Runway Closure`
- `Runway Contamination`
- `Runway Surface Condition`
- `Runway Friction Values`
- `Runway Length Change`
- `Displaced Threshold`
- `Taxiway Closure`
- `Taxiway Restriction`
- `Taxiway Light U/S`
- `Taxiway Marking`
- `Apron Closure`
- `Gate Restriction`
- `Parking Limitation`
- `Pushback Restriction`
- `SNOWTAM`
- `Braking Action Report`
- `Surface Contaminant`
- `Terminal Infrastructure`
- `Ground Equipment`
- `Airport Utilities`
- `Movement Area Vehicle`
- `Construction Zone`
- `Maintenance Activity`
- 'Wing Span Restriction'

## Airspace Management Tags
- `Restricted Airspace Active`
- `Danger Area Active`
- `Prohibited Area Active`
- `Temporary Flight Restriction`
- `Special Use Airspace`
- `Security Airspace`
- `Military Activity`
- `Missile Firing`
- `Artillery Exercise`
- `UAS/Drone Activity`
- `Flow Control`
- `Traffic Management`
- `Slot Restriction`
- `Airspace Delay`
- `Airspace Classification Change`
- `Airspace Boundary Change`
- `Airshow`
- `Sporting Event`
- `VIP Movement`

## Navigation Systems Tags
- `ILS U/S`
- `ILS Degraded`
- `MLS U/S`
- `GLS U/S`
- `VOR U/S`
- `DME U/S`
- `NDB U/S`
- `Marker Beacon U/S`
- `GNSS Outage`
- `GPS Interference`
- `GALILEO Outage`
- `GLONASS Outage`
- `PAPI U/S`
- `VASI U/S`
- `Approach Light U/S`
- `Lead-in Light U/S`
- `Runway Light U/S`
- `New Instrument Procedure`
- `Approach Procedure Change`
- `Missed Approach Change`
- `Radar Coverage`
- `Surveillance Limitation`
- `ADS-B Coverage`

## Communication Services Tags
- `Tower Frequency Change`
- `Ground Frequency Change`
- `Approach Frequency Change`
- `Center Frequency Change`
- `ATIS U/S`
- `ATIS Frequency Change`
- `VOLMET U/S`
- `Emergency Frequency Issue`
- `Guard Frequency Problem`
- `SAR Communication`
- `CPDLC Availability`
- `ACARS Service`
- `PDC/DCL Service`
- `ILS Frequency Change`
- `VOR Frequency Change`
- `DME Frequency Change`
- `Transmitter Outage`
- `Receiver Outage`

## Operational Procedures Tags
- `SID Procedure Change`
- `STAR Procedure Change`
- `Departure Restriction`
- `Climb Gradient Change`
- `Arrival Restriction`
- `Holding Pattern Change`
- `Speed Restriction`
- `Approach Minima Changed`
- `Circling Restriction`
- `RNP Requirement`
- `Taxi Route Change`
- `Hot Spot Advisory`
- `Low Vis Procedure`
- `Emergency Procedure Change`
- `Evacuation Route`
- `Assembly Point`
- `Crash Gate Location`
- `Noise Abatement`
- `Wake Turbulence Procedure`
- `Slot Time Required`

## Obstacles and Hazards Tags
- `Unlit Obstacle`
- `New Crane`
- `New Tower`
- `Temporary Structure`
- `Obstacle Light Failure`
- `Reduced Intensity Lighting`
- `Construction Hazard`
- `Work Area Active`
- `Height Restriction`
- `Tree Hazard`
- `Terrain Change`
- `Wildlife Concentration`
- `Temporary Obstruction`
- `Balloon Activity`
- `Banner Tow`
- `Cable/Wire Hazard`
- `Laser Activity`
- `Light Show`

## Weather and Environmental Tags
- `AWOS/ASOS U/S`
- `RVR Equipment U/S`
- `Wind Indicator U/S`
- `Weather Equipment Failure`
- `CAT II/III Unavailable`
- `Low Visibility Operation`
- `Windshear Alert`
- `Microburst Warning`
- `Severe Weather Advisory`
- `Volcanic Ash`
- `Smoke Hazard`
- `Dust Storm`
- `Industrial Emission`
- `De-icing Facility Limited`
- `De-icing Unavailable`
- `Snow Removal Operation`
- `Heating System U/S`
- `Weather Briefing Limited`
- `Forecast Limitation`

## Safety and Security Tags
- `ARFF Index Change`
- `Fire/Rescue Limited`
- `Medical Service Limited`
- `Emergency Service Degraded`
- `Security Screening Change`
- `Restricted Area`
- `ID Requirement Change`
- `Arrestor System U/S`
- `EMAS U/S`
- `Safety Area Compromised`
- `Bird Activity`
- `Wildlife Hazard`
- `Wildlife Control Active`
- `Dangerous Goods Restriction`
- `Storage Limitation`
- `Security Threat Level`
- `Special Security Procedure`
- `Escort Required`

## Infrastructure Services Tags
- `Fuel Unavailable`
- `100LL Restriction`
- `JET-A Restriction`
- `Fuel Quality Issue`
- `Fuel Rationing`
- `GPU Unavailable`
- `Air Start Limited`
- `Pushback Service Limited`
- `Towing Limited`
- `Catering Unavailable`
- `Water Service Limited`
- `Lavatory Service Limited`
- `Cabin Cleaning Limited`
- `Gate Bridge U/S`
- `Bus Service Required`
- `Terminal Facility Limited`
- `Hangar Unavailable`
- `AOG Support Limited`
- `Parts Availability`
- `Electrical Power Limited`
- `Airport Lighting Limited`
- `Water System Issue`

## Administrative Tags
- `AIP Update`
- `Chart Change`
- `Procedure Amendment`
- `Publication Change`
- `Authority Change`
- `Inspector Availability`
- `Certification Issue`
- `Handling Agent Change`
- `Customs Hours`
- `Immigration Hours`
- `Flight Planning Facility`
- `Briefing Service Change`
- `Weather Service Change`
- `NOF Address Change`
- `Landing Fee Change`
- `Parking Fee Change`
- `Service Fee Update`
- `Payment Method Change`
- `Political Situation`
- `Health Requirement`
- `Special Event Notice`

## Time-Based Filter Tags
- `Immediate Effect`
- `Urgent (72hr)`
- `Routine`
- `Future Planned`
- `Permanent Change`
- `Long Term (>90 days)`
- `Medium Term (7-90 days)`
- `Short Term (<7 days)`
- `Daily Schedule`
- `Event Specific`

## Flight Phase Filter Tags
- `Affects Preflight`
- `Affects Taxi`
- `Affects Takeoff`
- `Affects Cruise`
- `Affects Approach`
- `Affects Landing`
- `Affects Ground Ops`
- `Affects All Phases`

## Aircraft Type Filter Tags
- `All Aircraft`
- `Commercial Aviation`
- `General Aviation`
- `Helicopter Only`
- `Military Only`
- `Large Aircraft Only`
- `Small Aircraft Only`
- `Special Operations`

## Special Condition Tags
- `Winter Operations`
- `Summer Construction`
- `Monsoon Conditions`
- `Night Operations Only`
- `Day Operations Only`
- `VMC Only`
- `IMC Impact`
- `Multi-Category NOTAM`
- `Safety Critical`
- `Emergency Response Impact`

## Location-Based Tags
- `Runway Specific`
- `Taxiway Specific`
- `Terminal Area`
- `Approach Area`
- `Departure Area`
- `FIR Wide`
- `Airport Wide`
- `Multiple Airports`

## Total: 275+ Operational Filter Tags

### Implementation Notes:

Output format:
- ISO 8601 timestamps (e.g., 2024-09-06T14:00:00Z)
- Categories: 'FIR' or 'Airport'
- Aircraft types: 'Fixed Wing', 'Helicopter', or 'Both'
- Scenarios: 'Departure', 'Arrival', or 'Both'
- Location: Do not mention the area by coordinate if the NOTAM does not provide it explicitly
- 
"""

# Prompt template
notam_analysis_prompt = ChatPromptTemplate.from_messages([
    ("system", notam_analysis_system_msg),
    ("human", '"NOTAM issue datetime": {issued_date}\n\n"NOTAM text":\n\n{context}')
])

# Main function to call LLM
async def analyze_notam(text: str,date: str) -> Notam_Analysis:
    try:
        runnable = notam_analysis_prompt | llm.with_structured_output(Notam_Analysis)
        result = await runnable.ainvoke({
            "context": text,
            "issued_date": date
        })

        print("📊 Analysis Result:")
        print(result.model_dump_json(indent=2))
        return result
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        return None


