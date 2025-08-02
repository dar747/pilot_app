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

Level 1: Advisory (Flight planning, publications, political, VFR)
Level 2: Operational (apron closed, fuel issues, ATIS U/S, SID/STAR changes, UAS/drone activity)
Level 3: Critical (Runway/ILS/NAV U/S, GNSS outage, minima change, PAPI U/S, airport closure)

Use only these tags:
Low Vis Procedure, Runway Light U/S, Taxiway Light U/S, Nav Frequency Change, Comm Frequency Change, ILS U/S, DME U/S, VOR U/S, Navaid U/S, Flight Planning Procedure, Unlit Obstacle, Airport Obstacle, Displaced Threshold, Change of Runway Length, Change of Safety Altitude, VFR Flight, Tower Operating Hour, SID/STAR Procedure Change, Missed Approach Procedure Change, Approach Minima Changed, Restricted Airspace Active, Danger Area Active, GNSS Outage

Output format:
- ISO 8601 timestamps (e.g., 2024-09-06T14:00:00Z)
- Categories: 'FIR' or 'Airport'
- Aircraft types: 'Fixed Wing', 'Helicopter', or 'Both'
- Scenarios: 'Departure', 'Arrival', or 'Both'
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


