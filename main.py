from fastapi import FastAPI, Query
from typing import List, Optional
from db import SessionLocal, NotamRecord
from fastapi.middleware.cors import CORSMiddleware
from db import DATABASE_URL  # Add this at the top
import asyncio
import os

if not os.path.exists("notams.db"):
    from scheduler import build_and_populate_db
    asyncio.run(build_and_populate_db())


app = FastAPI(
    title="NOTAM Analysis API",
    description="Serve analyzed NOTAMs from the database",
    version="1.0.0"
)

# Allow access from frontend if needed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/notams", response_model=List[dict])
def get_all_notams(
    airport: Optional[str] = Query(None, description="Filter by ICAO airport code"),
    seriousness: Optional[int] = Query(None, ge=1, le=3, description="Filter by seriousness level (1-3)"),
    limit: int = Query(100, ge=1, le=500, description="Limit number of results")
):
    session = SessionLocal()
    query = session.query(NotamRecord)

    if airport:
        query = query.filter(NotamRecord.airport == airport.upper())

    if seriousness:
        query = query.filter(NotamRecord.seriousness == seriousness)

    notams = query.limit(limit).all()
    session.close()

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

    return [format(n) for n in notams]

@app.get("/debug")
def debug_db():
    print(f"📁 Using database: {DATABASE_URL}")  # ⬅️ Print DB path
    session = SessionLocal()
    records = session.query(NotamRecord).all()
    session.close()
    return {"count": len(records)}


@app.get("/")
def root():
    return {"message": "NOTAM API is running. Use /notams to query."}


import inspect

print("\n✅ ROUTES LOADED:")
for route in app.routes:
    print(f"{route.path} → {route.name}")
