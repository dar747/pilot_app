# generate_briefing.py
from typing import Optional
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import joinedload
from notam.db import Airport
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from notam.models import Notam_Briefing, Notam_Query_User_Input_Parser
from notam.db import NotamRecord
from datetime import datetime,timezone
from langsmith import Client

load_dotenv()

os.environ["LANGCHAIN_TRACING_V2"] = "true"
langchain_api_key = os.getenv("LANGCHAIN_API_KEY")
os.environ["LANGCHAIN_PROJECT"] = "Pilot_App_Generate_Briefing"


langsmith_client = Client()  # No need to pass api_key


SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
engine = create_engine(SUPABASE_DB_URL)
SessionLocal = sessionmaker(bind=engine)

openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    raise ValueError("OPENAI_API_KEY is not set in environment.")



llm = ChatOpenAI(model="gpt-5-mini", api_key=openai_api_key)
llm_o4_mini = ChatOpenAI(model="o4-mini", api_key=openai_api_key)

notam_analyse_user_input_system_msg = ("""You are an excellent middleman good at analysing an Pilot inquiry on their flight scenario. 
You must accurately extract the ICAO code and interested timeframe of the interested airport and also scenario.
The time must in the calculated time that affects the scenario. For example, if the user is interested in a scenario 3 hours from now. You must get the corresponding time in UTC. If no time is provided in the inquiry, you must also include the UTC time now in scenario just in case the user is interested instant situation.""")

notam_analyse_user_input_prompt = ChatPromptTemplate.from_messages([
    ("system", notam_analyse_user_input_system_msg),
    ("human", '"Time now" is {time} UTC. \n\n "User input" \n\n{context}')
]).partial(time=datetime.now(timezone.utc))


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
        query = session.query(NotamRecord).join(NotamRecord.airports).filter(
            Airport.icao_code == airport.upper()
        )

        if active_only:
            now = datetime.now(timezone.utc).isoformat()
            query = query.filter(
                NotamRecord.start_time <= now,
                NotamRecord.end_time >= now
            )
        query = query.options(
            joinedload(NotamRecord.airports),
            joinedload(NotamRecord.operational_tags),
            joinedload(NotamRecord.filter_tags)
        )

        return query.all()
    finally:
        session.close()

# System instruction with classification guidance
# notam_briefing_system_msg = """
# You are an aviation operations specialist. Your job is to write a professional, human-readable NOTAM briefing for pilots and flight dispatchers.
#
# Given the flight scenario, a NOTAM message and the interested timeframe, generate detailed briefing in natural language that answers:
# - Categorize the types of NOTAM based on what will influence the scenario the most. Timeframe is important as to whether the NOTAM is important. You should consider margin of time as well just in case the flight may happen earlier or delay.
# If no Scenario is given, then you should analyse based on phase of flight, Taxi, Departure, Enroute, Arrival if appropriate and type of traffic.
#
# DO NOT repeat the original NOTAM message word-for-word. Summarize it clearly.
#
# At the end of your response, you must indicate the NOTAM number that affects the flight scenario.
#
# """
#
# # Prompt template
# notam_briefing_prompt = ChatPromptTemplate.from_messages([
#     ("system", notam_briefing_system_msg),
#     ("human", """ -User interested Scenario: {flight_scenario} \n\n"NOTAM Messages":\n\n{context}""")
# ])

notam_briefing_prompt = langsmith_client.pull_prompt("notam_briefing_prompt",include_model =True)
# Main function to call LLM
async def notam_briefing(text: str,scenario: str) -> Notam_Briefing:

    try:
        runnable = notam_briefing_prompt
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


if __name__ == "__main__":
    import asyncio

    async def main():
        result = await briefing_chain("Take off from VHHH")
        print(result)

    asyncio.run(main())