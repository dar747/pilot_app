# main.py
from dotenv import load_dotenv
load_dotenv()
import os

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

from sqlalchemy import create_engine, or_, select
from sqlalchemy.orm import sessionmaker, selectinload

from notam.generate_briefing import briefing_chain
from notam.db import (
    NotamRecord, Airport, OperationalTag,
    # enums
    SeverityLevelEnum, NotamCategoryEnum, PrimaryCategoryEnum,
)

# -----------------------------------------------------------------------------
# DB setup
# -----------------------------------------------------------------------------
#os.environ["DATABASE_URL"] = os.getenv("SUPABASE_DB_URL") or os.getenv("LOCAL_DB_URL") or ""
os.environ["DATABASE_URL"] = os.getenv("LOCAL_DB_URL")
#DATABASE_URL = os.environ["DATABASE_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is empty. Set SUPABASE_DB_URL or LOCAL_DB_URL.")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
print(f"🔌 Using DATABASE_URL: {DATABASE_URL}")
print("Loaded API file:", __file__)

# -----------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------
app = FastAPI(title="NOTAM Analysis API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _enum_val(v):
    """Return enum's value or the value itself if already a string/None."""
    return getattr(v, "value", v)

def format_notam(record: NotamRecord) -> Dict[str, Any]:
    """Minimal JSON shape for the app (extend as needed)."""
    def designator(r):
        return f"{r.runway_number}{r.runway_side or ''}"

    # assume at most one condition per runway
    affected_runways = []
    for r in record.runways:
        # try to find a matching condition (naive: same runway_number/side)
        cond = next(
            (c for c in record.runway_conditions
             if c.runway_number == r.runway_number and c.runway_side == r.runway_side),
            None
        )
        affected_runways.append({
            "runway": designator(r),
            "friction_value": getattr(cond, "friction_value", None) if cond else None,
            # you can add more condition fields here
        })

    return {
        "id": record.id,
        "notam_number": record.notam_number,
        "issue_time": record.issue_time,
        "notam_category": _enum_val(record.notam_category),
        "severity_level": _enum_val(record.severity_level),
        "start_time": record.start_time,
        "end_time": record.end_time,
        "time_classification": _enum_val(record.time_classification),
        "time_of_day_applicability": _enum_val(record.time_of_day_applicability),
        "flight_rule_applicability": _enum_val(record.flight_rule_applicability),
        "primary_category": _enum_val(record.primary_category),
        "affected_area": record.affected_area,
        "affected_airports_snapshot": record.affected_airports_snapshot,
        "notam_summary": record.notam_summary,
        "icao_message": record.icao_message,
        "replacing_notam": record.replacing_notam,
        "airports": [a.icao_code for a in record.airports],
        "operational_tags": [t.tag_name for t in record.operational_tags],
        "flight_phases":[p.phase for p in record.flight_phase_links],
     #   "obstacles":
        "affected_runways": affected_runways,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "affected_aircraft": {
            "sizes": [_enum_val(s) for s in record.aircraft_sizes],
            "propulsions": [_enum_val(p) for p in record.aircraft_propulsions],
            "wingspan": {
                "min_m": record.wingspan_restriction.min_m if record.wingspan_restriction else None,
                "max_m": record.wingspan_restriction.max_m if record.wingspan_restriction else None,
                "min_inclusive": record.wingspan_restriction.min_inclusive if record.wingspan_restriction else None,
                "max_inclusive": record.wingspan_restriction.max_inclusive if record.wingspan_restriction else None,
            } if record.wingspan_restriction else None,
        },
        "base_score": record.base_score,


    }

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/")
def root():
    return {"message": "✅ NOTAM API is running. Use /notams to query."}

@app.get("/ping")
def ping():
    return {"message": "pong"}

@app.get("/check-db")
def check_db_connection():
    session = SessionLocal()
    try:
        db_url = str(session.get_bind().url)
        count = session.query(NotamRecord).count()
        return {"message": "✅ DB OK", "record_count": count, "connected_to": db_url}
    except Exception as e:
        return {"error": str(e)}
    finally:
        session.close()

@app.get("/notams", response_model=List[dict])
def get_all_notams(
    airport: Optional[str] = Query(None, description="Filter by ICAO code"),
    severity_level: Optional[str] = Query(None, description="CRITICAL, OPERATIONAL, ADVISORY"),
    notam_category: Optional[str] = Query(None, description="FIR or AIRPORT"),
    primary_category: Optional[str] = Query(None, description="RUNWAY_OPERATIONS, AERODROME_OPERATIONS, ..."),
    start_time_after: Optional[datetime] = Query(None),
    end_time_before: Optional[datetime] = Query(None),
    operational_tag: Optional[str] = Query(None, description="substring match on tag name"),
    replacing_notam: Optional[str] = Query(None),
    active_only: Optional[bool] = Query(False, description="only currently active NOTAMs"),
    keyword: Optional[str] = Query(None, description="search NOTAM summary text"),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
):
    session = SessionLocal()
    try:
        q = (
            session.query(NotamRecord)
            .options(
                # eager-load to avoid N+1
                selectinload(NotamRecord.airports),
                selectinload(NotamRecord.operational_tags),
                selectinload(NotamRecord.runways),
                selectinload(NotamRecord.runway_conditions),
                selectinload(NotamRecord.flight_phase_links),
                selectinload(NotamRecord.wingspan_restriction),
                selectinload(NotamRecord.aircraft_size_links),
                selectinload(NotamRecord.aircraft_propulsion_links),


            )
        )

        if airport:
            q = q.join(NotamRecord.airports).filter(Airport.icao_code == airport.upper())

        if severity_level:
            try:
                sev = SeverityLevelEnum(severity_level.upper())
                q = q.filter(NotamRecord.severity_level == sev)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid severity_level")

        if notam_category:
            try:
                cat = NotamCategoryEnum(notam_category.upper())
                q = q.filter(NotamRecord.notam_category == cat)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid notam_category")

        if primary_category:
            try:
                pc = PrimaryCategoryEnum(primary_category.upper())
                q = q.filter(NotamRecord.primary_category == pc)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid primary_category")

        if start_time_after:
            q = q.filter(NotamRecord.start_time >= start_time_after)

        if end_time_before:
            q = q.filter(or_(NotamRecord.end_time <= end_time_before, NotamRecord.end_time.is_(None)))

        if operational_tag:
            q = q.filter(NotamRecord.operational_tags.any(OperationalTag.tag_name.ilike(f"%{operational_tag}%")))

        if replacing_notam:
            q = q.filter(NotamRecord.replacing_notam == replacing_notam)

        if keyword:
            q = q.filter(NotamRecord.notam_summary.ilike(f"%{keyword}%"))

        if active_only:
            now = datetime.now(timezone.utc)
            q = q.filter(NotamRecord.start_time <= now).filter(
                or_(NotamRecord.end_time.is_(None), NotamRecord.end_time >= now)
            )

        q = q.order_by(NotamRecord.start_time.desc(), NotamRecord.issue_time.desc())
        rows = q.offset(offset).limit(limit).all()

        return [format_notam(n) for n in rows]
    finally:
        session.close()

@app.get("/briefing-from-input")
async def get_briefing_from_input(query: str):
    return await briefing_chain(query)
