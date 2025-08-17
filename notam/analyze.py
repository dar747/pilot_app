# analyze.py

import os
from dotenv import load_dotenv

load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from notam.models import Notam_Analysis

llm = ChatOpenAI(model="gpt-5-mini", api_key=openai_api_key)

# System instruction with classification guidance
notam_analysis_system_msg = """
You are an expert in categorizing NOTAMs based on seriousness and extracting structured data.

Severity levels
-Advisory – Info only; no change to availability/minima (e.g., publications, political notes, general VFR info).
-Operational – Planning or tactical adjustment required; availability reduced but not lost (e.g., runway shortened, taxiway closed with alternatives, partial lighting U/S, fuel limited).
-Critical – Potential safety impact or loss of capability (e.g., runway/airport closed, ILS/NAVAID U/S, GNSS outage/jamming, RVR or approach lights U/S in IMC, ARFF downgrade below required index, TFR/Prohibited active, braking poor).

Prefer operational availability over cause. Tag the consequence, not the reason.

Time classification
PERMANENT
LONG_TERM (>90 days)
MEDIUM_TERM (7–90 days)
SHORT_TERM (<7 days)
DAILY / WEEKLY / MONTHLY
EVENT_SPECIFIC (one-off)

All times must be ISO-8601 UTC (e.g., 2025-08-09T14:00:00Z). If the NOTAM lacks explicit coordinates, omit them.
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


