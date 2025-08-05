from notam.generate_briefing import briefing_chain
from fastapi import FastAPI, Query
from typing import List, Optional
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
from notam.db import NotamRecord, Airport, OperationalTag, FilterTag # Don't import SessionLocal from db.py
import os
from dotenv import load_dotenv
load_dotenv()

DATABASE_URL = os.getenv("SUPABASE_DB_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
print(f"🔌 Using DATABASE_URL: {DATABASE_URL}")


app = FastAPI(title="NOTAM Analysis API", version="1.0.0")
print("APP LOADED ✅")


# Allow cross-origin requests (adjust in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Utility to format response
def format(record):
    return {
        "notam_number": record.notam_number,
        "issue_time": record.issue_time,
        "notam_info_type": record.notam_info_type,
        "notam_category": record.notam_category,
        "start_time": record.start_time,
        "end_time": record.end_time,
        "seriousness": record.seriousness,
        "severity_level": str(record.severity_level),
        "urgency_indicator": str(record.urgency_indicator),
        "applied_scenario": record.applied_scenario,
        "applied_aircraft_type": record.applied_aircraft_type,
        "notam_summary": record.notam_summary,
        "icao_message": record.icao_message,
        "confidence_score": record.confidence_score,
        "affected_area": record.affected_area,
        "replacing_notam": record.replacing_notam,
        "airports": [a.icao_code for a in record.airports],
        "operational_tags": [t.tag_name for t in record.operational_tags],
        "filter_tags": [t.tag_name for t in record.filter_tags],
    }
@app.get("/")
def root():
    return {"message": "✅ NOTAM API is running. Use /notams to query."}


@app.get("/notams", response_model=List[dict])
def get_all_notams(
    airport: Optional[str] = Query(None),
    seriousness: Optional[int] = Query(None, ge=1, le=3),
    severity_level: Optional[str] = Query(None, description="CRITICAL, OPERATIONAL, ADVISORY"),
    urgency_indicator: Optional[str] = Query(None, description="IMMEDIATE, URGENT, ROUTINE, PLANNED"),
    notam_info_type: Optional[str] = None,
    notam_category: Optional[str] = None,
    start_time_after: Optional[datetime] = None,
    end_time_before: Optional[datetime] = None,
    applied_scenario: Optional[str] = None,
    applied_aircraft_type: Optional[str] = None,
    operational_tag: Optional[str] = None,
    filter_tag: Optional[str] = None,
    replacing_notam: Optional[str] = None,
    active_only: Optional[bool] = False,
    keyword: Optional[str] = Query(None, description="Search in NOTAM summary"),
    min_confidence: Optional[float] = Query(None, ge=0.0, le=1.0),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500)
):
    session = SessionLocal()
    try:
        query = session.query(NotamRecord)

        if airport:
            query = query.join(NotamRecord.airports).filter(Airport.icao_code == airport.upper())
        if seriousness:
            query = query.filter(NotamRecord.seriousness == seriousness)
        if severity_level:
            query = query.filter(NotamRecord.severity_level == severity_level.upper())
        if urgency_indicator:
            query = query.filter(NotamRecord.urgency_indicator == urgency_indicator.upper())
        if notam_info_type:
            query = query.filter(NotamRecord.notam_info_type == notam_info_type)
        if notam_category:
            query = query.filter(NotamRecord.notam_category == notam_category)
        if start_time_after:
            query = query.filter(NotamRecord.start_time >= start_time_after)
        if end_time_before:
            query = query.filter(NotamRecord.end_time <= end_time_before)
        if applied_scenario:
            query = query.filter(NotamRecord.applied_scenario == applied_scenario)
        if applied_aircraft_type:
            query = query.filter(NotamRecord.applied_aircraft_type == applied_aircraft_type)
        if operational_tag:
            query = query.filter(NotamRecord.operational_tags.any(OperationalTag.tag_name.ilike(f"%{operational_tag}%")))
        if filter_tag:
            query = query.filter(NotamRecord.filter_tags.any(FilterTag.tag_name.ilike(f"%{filter_tag}%")))
        if replacing_notam:
            query = query.filter(NotamRecord.replacing_notam == replacing_notam)
        if keyword:
            query = query.filter(NotamRecord.notam_summary.ilike(f"%{keyword}%"))
        if min_confidence is not None:
            query = query.filter(NotamRecord.confidence_score >= min_confidence)
        if active_only:
            now = datetime.now(timezone.utc)
            query = query.filter(NotamRecord.start_time <= now, NotamRecord.end_time >= now)

        notams = query.offset(offset).limit(limit).all()
        return [format(n) for n in notams]
    finally:
        session.close()

@app.get("/ping")
def ping():
    return {"message": "pong"}

@app.get("/check-db")
def check_db_connection():
    session = SessionLocal()
    try:
        db_url = str(session.get_bind().url)
        count = session.query(NotamRecord).count()
        return {
            "message": "✅ DB OK",
            "record_count": count,
            "connected_to": db_url  # <- this will prove it's local or Supabase
        }
    except Exception as e:
        return {"error": str(e)}
    finally:
        session.close()




@app.get("/briefing-from-input")
async def get_briefing_from_input(query: str):
    return await briefing_chain(query)