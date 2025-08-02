# generate_briefing.py
from typing import Optional
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_API_KEY"] = "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"
os.environ["LANGCHAIN_PROJECT"] = "PILOT"

load_dotenv()

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
engine = create_engine(SUPABASE_DB_URL)
SessionLocal = sessionmaker(bind=engine)

openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    raise ValueError("OPENAI_API_KEY is not set in environment.")

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from notam.models import Notam_Briefing, Notam_Query_User_Input_Parser
from notam.db import NotamRecord
from datetime import datetime

llm = ChatOpenAI(model="gpt-4.1-mini", temperature=0, api_key=openai_api_key)

notam_analyse_user_input_system_msg = "Extract the ICAO code of the interested airport and also scenario"

notam_analyse_user_input_prompt = ChatPromptTemplate.from_messages([
    ("system", notam_analyse_user_input_system_msg),
    ("human", '"User input" \n\n{context}')
])


async def analyse_user_input(text:str)-> Optional[Notam_Query_User_Input_Parser]:

    try:
        runnable = notam_analyse_user_input_prompt | llm.with_structured_output(Notam_Query_User_Input_Parser)
        result = await runnable.ainvoke({
            "context": text,
        })
        print("📊 Extracted Result:")
        print(result.model_dump_json(indent=2))
        return result
    except Exception as e:
        print(f"❌ Briefing failed: {e}")
        return None

def get_notams_by_airport(airport: str, active_only=True):
    session = SessionLocal()
    try:
        query = session.query(NotamRecord).filter(NotamRecord.airport == airport.upper())

        if active_only:
            now = datetime.utcnow().isoformat()
            query = query.filter(
                NotamRecord.start_time <= now,
                NotamRecord.end_time >= now
            )

        return query.all()
    finally:
        session.close()

# System instruction with classification guidance
notam_briefing_system_msg = """
You are an aviation operations specialist. Your job is to write a professional, human-readable NOTAM briefing for pilots and flight dispatchers.

Given the flight scenario,  a NOTAM message and issue time, generate detailed briefing in natural language that answers:

- What is affected?
- Where is the impact?
- When is it in effect (start/end)?
- Who is impacted (e.g., fixed wing, helicopters)?
- Why is this operationally important?

DO NOT repeat the original NOTAM message word-for-word. Summarize it clearly.

Include the following sections in the briefing:
- Summary
- Operational Impact
- Affected Time Window (in UTC)
- Replacing NOTAM (if applicable)

Respond in this format:
briefing: "<Natural language summary>

"""

# Prompt template
notam_briefing_prompt = ChatPromptTemplate.from_messages([
    ("system", notam_briefing_system_msg),
    ("human", '"-Flight Scenario: {flight_scenario} \n\n"NOTAM Messages":\n\n{context}')
])

# Main function to call LLM
async def notam_briefing(text: str,scenario: str) -> Notam_Briefing:

    try:
        runnable = notam_briefing_prompt | llm.with_structured_output(Notam_Briefing)
        result = await runnable.ainvoke({
            "context": text,
            "flight_scenario": scenario
        })

        print("📊 Briefing Result:")
        print(result.model_dump_json(indent=2))
        return result
    except Exception as e:
        print(f"❌ Briefing failed: {e}")
        return None


async def briefing_chain(user_input: str) -> dict:
    # Step 1: Use LLM to extract airport + scenario
    parsed = await analyse_user_input(user_input)

    if not parsed or not parsed.airport or not parsed.flight_scenario:
        return {"error": "Could not extract airport and scenario from input."}

    airport = parsed.airport
    scenario = parsed.flight_scenario

    # Step 2: Fetch NOTAMs from Supabase
    notams = get_notams_by_airport(airport)
    if not notams:
        return {"error": f"No NOTAMs found for {airport}"}

    # Step 3: Combine messages for briefing
    text = "\n\n".join(f"{n.notam_number}: {n.icao_message}" for n in notams)

    # Step 4: Run briefing generator
    result = await notam_briefing(text, scenario)

    return result.model_dump() if result else {"error": "Briefing failed"}

