# generate_briefing.py
from typing import Optional
import os
import json
from datetime import datetime, timezone

from dotenv import load_dotenv
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker, joinedload

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langsmith import Client

from notam.db import Airport, NotamRecord
from notam.models import Notam_Briefing, Notam_Query_User_Input_Parser

# --- Env & clients ---
load_dotenv()

os.environ["LANGCHAIN_TRACING_V2"] = "true"
langchain_api_key = os.getenv("LANGCHAIN_API_KEY")
os.environ["LANGCHAIN_PROJECT"] = "Pilot_App_Generate_Briefing"

langsmith_client = Client()

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_DEV_URL")
if not SUPABASE_DB_URL:
    raise ValueError("SUPABASE_DB_URL is not set in environment.")
engine = create_engine(SUPABASE_DB_URL)
SessionLocal = sessionmaker(bind=engine)

openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    raise ValueError("OPENAI_API_KEY is not set in environment.")

llm = ChatOpenAI(model="gpt-5-mini", api_key=openai_api_key)
# If you want a stronger model for the briefing step:
# llm = ChatOpenAI(model="o4-mini", api_key=openai_api_key)

# --- Analyse user input prompt ---
notam_analyse_user_input_system_msg = (
    "You are an excellent middleman good at analysing a Pilot inquiry on their flight scenario. "
    "You must accurately extract the ICAO code and interested timeframe of the interested airport and also scenario. "
    "The time must be in the calculated time that affects the scenario. For example, if the user is interested in a "
    "scenario 3 hours from now, you must get the corresponding time in UTC. If no time is provided, also include UTC now."
)

notam_analyse_user_input_prompt = ChatPromptTemplate.from_messages([
    ("system", notam_analyse_user_input_system_msg),
    ("human", '"Time now" is {time} UTC.\n\n"User input"\n\n{context}')
]).partial(time=datetime.now(timezone.utc).isoformat())

# --- Briefing prompt (your inline version) ---
# System instruction with classification guidance
notam_briefing_system_msg = """
You are an aviation operations specialist. Your job is to write a professional, human-readable NOTAM briefing for pilots and flight dispatchers.

Given the flight scenario, a NOTAM message and the interested timeframe, generate detailed briefing in natural language that answers:
- Categorize the types of NOTAM based on what will influence the scenario the most. Timeframe is important as to whether the NOTAM is important. You should consider margin of time as well just in case the flight may happen earlier or delay.
If no Scenario is given, then you should analyse based on phase of flight, Taxi, Departure, Enroute, Arrival if appropriate and type of traffic.

DO NOT repeat the original NOTAM message word-for-word. Summarize it clearly.

At the end of your response, you must indicate the NOTAM number that affects the flight scenario.
"""

notam_briefing_prompt = ChatPromptTemplate.from_messages([
    ("system", notam_briefing_system_msg),
    ("human", "-User interested Scenario: {flight_scenario}\n\n\"NOTAM Messages\":\n\n{context}")
])

# If you prefer using a stored LangSmith prompt, pull it WITHOUT an embedded model:
# notam_briefing_prompt = langsmith_client.pull_prompt("notam_briefing_prompt", include_model=False)

# --- Functions ---
async def analyse_user_input(text: str) -> Optional[Notam_Query_User_Input_Parser]:
    """Return a Pydantic object with airport & scenario."""
    try:
        runnable = notam_analyse_user_input_prompt | llm.with_structured_output(Notam_Query_User_Input_Parser)
        result = await runnable.ainvoke({"context": text})
        print("📊 Extracted Result:")
        print(result.model_dump_json(indent=2))
        return result
    except Exception as e:
        print(f"❌ analyse_user_input failed: {e}")
        return None


def get_notams_by_airport(airport: str, active_only: bool = True):
    """Fetch NOTAM records for an airport; optionally only active ones."""
    session = SessionLocal()
    try:
        query = (
            session.query(NotamRecord)
            .join(NotamRecord.airports)
            .filter(Airport.icao_code == airport.upper())
        )

        if active_only:
            now = datetime.now(timezone.utc)
            query = query.filter(
                NotamRecord.start_time <= now,
                or_(NotamRecord.end_time.is_(None), NotamRecord.end_time >= now),
            )

        query = query.options(
            joinedload(NotamRecord.airports),
            joinedload(NotamRecord.operational_tags),
        )

        return query.all()
    finally:
        session.close()


async def notam_briefing(text: str, scenario: str) -> Optional[Notam_Briefing]:
    """Generate a structured NOTAM briefing as a Pydantic model."""
    try:
        runnable = notam_briefing_prompt | llm.with_structured_output(Notam_Briefing)
        result: Notam_Briefing = await runnable.ainvoke({
            "context": text,
            "flight_scenario": scenario
        })
        print("📊 Briefing Result:")
        print(result.model_dump_json(indent=2))
        return result
    except Exception as e:
        print(f"❌ notam_briefing failed: {e}")
        return None


async def briefing_chain(user_input: str) -> dict:
    """
    1) Parse user input -> airport & scenario (Pydantic)
    2) Get NOTAMs from DB
    3) Build text bundle
    4) Generate structured briefing (Pydantic), return as dict outward
    """
    parsed = await analyse_user_input(user_input)
    if not parsed or not parsed.airport or not parsed.flight_scenario:
        return {"error": "Could not extract airport and scenario from input."}

    airport = parsed.airport
    scenario = parsed.flight_scenario

    notams = get_notams_by_airport(airport)
    if not notams:
        return {"error": f"No NOTAMs found for {airport}"}

    text = "\n\n".join(f"{n.notam_number}: {n.icao_message}" for n in notams)

    result = await notam_briefing(text, scenario)
    return result.model_dump() if result else {"error": "Briefing failed"}


# --- CLI ---
if __name__ == "__main__":
    import asyncio

    async def main():
        result = await briefing_chain("Take off from VHHH in 2 hours")
        print(json.dumps(result, indent=2))

    asyncio.run(main())
