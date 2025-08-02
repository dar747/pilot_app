from fastapi import FastAPI, Query, HTTPException
from typing import List, Optional
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from db import Base, NotamRecord  # Don't import SessionLocal from db.py
import os

# Load Supabase DB URL from environment
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
if not SUPABASE_DB_URL:
    raise RuntimeError("SUPABASE_DB_URL is not set in environment variables")

# 🔁 Connect to Supabase instead of local DB
engine = create_engine(SUPABASE_DB_URL)
SessionLocal = sessionmaker(bind=engine)

# Create FastAPI app
app = FastAPI(
    title="NOTAM Analysis API",
    description="Serve analyzed NOTAMs from Supabase database",
    version="1.0.0"
)

# Allow cross-origin requests (adjust in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Format NOTAM record for response
def format(record):
    return {
        "notam_number": record.notam_number,
        "issue_time": record.issue_time,
        "notam_info_type": record.notam_info_type,
        "notam_category": record.notam_category,
        "airport": record.airport,
        "start_time": record.start_time,
        "end_time": record.end_time,
        "seriousness": record.seriousness,
        "applied_scenario": record.applied_scenario,
        "applied_aircraft_type": record.applied_aircraft_type,
        "operational_tag": record.operational_tag.split(",") if record.operational_tag else [],
        "affected_runway": record.affected_runway.split(",") if record.affected_runway else [],
        "notam_summary": record.notam_summary,
        "icao_message": record.icao_message,
        "replacing_notam": record.replacing_notam
    }

# Query endpoint with flexible filters
@app.get("/notams", response_model=List[dict])
def get_all_notams(
    airport: Optional[str] = None,
    seriousness: Optional[int] = Query(None, ge=1, le=3),
    notam_info_type: Optional[str] = None,
    notam_category: Optional[str] = None,
    start_time_after: Optional[str] = None,
    end_time_before: Optional[str] = None,
    applied_scenario: Optional[str] = None,
    applied_aircraft_type: Optional[str] = None,
    operational_tag: Optional[str] = None,
    affected_runway: Optional[str] = None,
    replacing_notam: Optional[str] = None,
    active_only: Optional[bool] = False,
    limit: int = Query(100, ge=1, le=500)
):
    session = SessionLocal()
    try:
        query = session.query(NotamRecord)

        if airport:
            query = query.filter(NotamRecord.airport == airport.upper())
        if seriousness:
            query = query.filter(NotamRecord.seriousness == seriousness)
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
            query = query.filter(NotamRecord.operational_tag.ilike(f"%{operational_tag}%"))
        if affected_runway:
            query = query.filter(NotamRecord.affected_runway.ilike(f"%{affected_runway}%"))
        if replacing_notam:
            query = query.filter(NotamRecord.replacing_notam == replacing_notam)
        if active_only:
            now = datetime.utcnow().isoformat()
            query = query.filter(NotamRecord.start_time <= now).filter(NotamRecord.end_time >= now)

        notams = query.limit(limit).all()
        return [format(n) for n in notams]
    finally:
        session.close()

# Root endpoint
@app.get("/")
def root():
    return {"message": "✅ NOTAM API is running. Use /notams to query."}
