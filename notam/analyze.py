# notam/analyze.py

import os
from dotenv import load_dotenv

load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from notam.models import Notam_Analysis

llm = ChatOpenAI(
    model="gpt-5-mini",
    api_key=openai_api_key,
    timeout=200,     # seconds; adjust if needed
    max_retries=0,  # IMPORTANT: we manage retries ourselves
)

# System instruction with classification guidance
notam_analysis_system_msg = """
You are an expert in categorizing NOTAMs based on seriousness and extracting structured data.

Severity levels
-Advisory – Info only; no change to availability/minima (e.g., publications, political notes, general VFR info).
-Operational – Planning or tactical adjustment required; availability reduced but not lost (e.g., runway shortened, taxiway closed with alternatives, partial lighting U/S, fuel limited).
-Critical – Potential safety impact or loss of capability (e.g., runway/airport closed, ILS/NAVAID U/S, GNSS outage/jamming, RVR or approach lights U/S in IMC, ARFF downgrade below required index, TFR/Prohibited active, braking poor).

Prefer operational availability over cause. Tag the consequence, not the reason.

All times must be ISO-8601 UTC (e.g., 2025-08-09T14:00:00Z). If the NOTAM lacks explicit coordinates, omit them.

If the NOTAM contains explicit date ranges or lists, expand them into individual operational_instances, one per active date.
However, **do not split continuous date ranges** (i.e., if the start date and end date have no gap between them). If the dates and times are consecutive (e.g., the end time of one period directly follows the start time of the next), treat them as a **single operational instance**. Only split into multiple instances if there are **gaps** between the dates and times.
If the NOTAM specifies daily start/end times, generate one instance per day using those times.
If multiple dates appear before a single time block, apply that time block to each of those dates, not just the last one.

For the one-line description, provide a very short plain-English summary so a pilot immediately understands the operational impact. 
Mention only the affected element (e.g., runway, taxiway, stand, apron, navaid, airspace) and the consequence and its extent. 
Do not include dates, times, references, identifiers, frequencies, or codes. Do not repeat the same information in different words. Keep it clear, concise, and operational.

<For Taxiway Code Extraction Guidance>
Include:
Alphabetic codes.
Alphanumeric codes.
Do not include:
Non-alphabetic codes.
Codes with directions or additional details.
Codes with extra descriptors attached.
</Taxiway Code Extraction Guidance>

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


