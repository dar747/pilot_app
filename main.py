# main.py
from dotenv import load_dotenv
load_dotenv()

import os
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker, selectinload

# Import auth components
from notam.auth import auth_router, get_current_user, get_optional_user, AuthUser

from notam.generate_briefing import briefing_chain
from notam.db import (
    NotamRecord, Airport, OperationalTag,
    NotamCategoryEnum, PrimaryCategoryEnum,
)
from pydantic import BaseModel
from typing import Optional

# -------------------- DB setup --------------------
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_DEV_URL")
if not DATABASE_URL:
    raise RuntimeError("Set DATABASE_URL (or SUPABASE_DB_DEV_URL).")

engine = create_engine(
    DATABASE_URL,
    future=True,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_recycle=1800,  # recycle idle conns every 30 min
    pool_timeout=30,
)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
print(f"🔌 Using DATABASE_URL: {DATABASE_URL}")

# -------------------- App --------------------
app = FastAPI(
    title="NOTAM Analysis API",
    version="1.0.0",
    description="Professional aviation NOTAM analysis and briefing system with user authentication"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include authentication routes
app.include_router(auth_router)

# -------------------- Helpers --------------------
def _enum_val(v):
    return getattr(v, "value", v)

def _to_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _z(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return _to_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")

def _parse_iso_utc(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    st = s.strip()
    if st.endswith(("Z", "z")):
        st = st[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(st)
    except Exception:
        return None
    return _to_utc(dt)

def _is_active_now(record: NotamRecord, now_utc: datetime) -> bool:
    if record.start_time and record.start_time > now_utc:
        return False
    if record.end_time and record.end_time < now_utc:
        return False

    ops = (record.operational_instance or {}).get("operational_instances") or []
    if ops:
        for sl in ops:
            s = _parse_iso_utc(sl.get("start_iso"))
            e = _parse_iso_utc(sl.get("end_iso"))
            if s and e and s <= now_utc <= e:
                return True
        return False
    return True

def format_notam(record: NotamRecord) -> Dict[str, Any]:
    def designator(r):
        return f"{r.runway_number}{r.runway_side or ''}"

    affected_runways = []
    rc_map = {(c.runway_number, c.runway_side): c for c in record.runway_conditions}
    for r in record.runways:
        cond = rc_map.get((r.runway_number, r.runway_side))
        affected_runways.append({
            "runway": designator(r),
            "friction_value": getattr(cond, "friction_value", None) if cond else None,
        })

    obstacles = []
    for o in record.obstacles:
        obstacles.append({
            "type": o.type,
            "height_agl_ft": o.height_agl_ft,
            "height_amsl_ft": o.height_amsl_ft,
            "location": (
                {"latitude": o.latitude, "longitude": o.longitude}
                if (o.latitude is not None and o.longitude is not None) else None
            ),
            "lighting": o.lighting,
            # runway_reference section removed - fields don't exist anymore
        })

    return {
        "id": record.id,
        "notam_number": record.notam_number,
        "issue_time": _z(record.issue_time),
        "notam_category": _enum_val(record.notam_category),
        "start_time": _z(record.start_time),
        "end_time": _z(record.end_time),
        "time_of_day_applicability": _enum_val(record.time_of_day_applicability),
        "flight_rule_applicability": _enum_val(record.flight_rule_applicability),
        "primary_category": _enum_val(record.primary_category),
        "affected_area": record.affected_area,
        "affected_airports_snapshot": record.affected_airports_snapshot,
        "notam_summary": record.notam_summary,
        "one_line_description": record.one_line_description,
        "icao_message": record.icao_message,
        "replacing_notam": record.replacing_notam,
        "airports": [a.icao_code for a in record.airports],
        "operational_tags": [t.tag_name for t in record.operational_tags],
        "flight_phases": [_enum_val(p.phase) for p in record.flight_phase_links],
        "affected_runways": affected_runways,
        "obstacles": obstacles,
        "operational_instances": (record.operational_instance or {}).get("operational_instances"),
        "created_at": _z(record.created_at),
        "updated_at": _z(record.updated_at),
        "affected_aircraft": {
            "sizes": [_enum_val(s) for s in record.aircraft_sizes],
            "propulsions": [_enum_val(p) for p in record.aircraft_propulsions],
            "wingspan": (
                {
                    "min_m": record.wingspan_restriction.min_m,
                    "max_m": record.wingspan_restriction.max_m,
                    "min_inclusive": record.wingspan_restriction.min_inclusive,
                    "max_inclusive": record.wingspan_restriction.max_inclusive,
                } if record.wingspan_restriction else None
            ),
        },
        "base_score_vfr": record.base_score_vfr,
        "base_score_ifr": record.base_score_ifr,
    }

# -------------------- Public Routes --------------------
@app.get("/")
def root():
    return {
        "message": "✅ NOTAM API is running. Use /auth/signin to login, then /notams to query.",
        "version": "1.0.0",
        "auth_required": True
    }

@app.get("/ping")
def ping():
    return {"message": "pong", "timestamp": datetime.now().isoformat()}

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat(), "service": "NOTAM Analysis API"}

# -------------------- Protected Routes (auth required) --------------------
@app.get("/check-db")
def check_db_connection(current_user: AuthUser = Depends(get_current_user)):
    session = SessionLocal()
    try:
        db_url = str(session.get_bind().url)
        count = session.query(NotamRecord).count()
        return {"message": "✅ DB OK", "record_count": count, "connected_to": db_url, "user": current_user.email}
    except Exception as e:
        return {"error": str(e)}
    finally:
        session.close()

@app.get("/airports/{airport}/notams", response_model=List[dict])
def list_notams_for_airport(
    airport: str,
    current_user: AuthUser = Depends(get_current_user),
    notam_category: Optional[NotamCategoryEnum] = Query(None, description="FIR or AIRPORT"),
    primary_category: Optional[PrimaryCategoryEnum] = Query(None),
    start_time_after: Optional[datetime] = Query(None, description="UTC time"),
    end_time_before: Optional[datetime] = Query(None, description="UTC time"),
    active_only: bool = Query(False, description="Coarse window + operational_instances"),
    include_inactive: bool = Query(False, description="Include replaced/cancelled NOTAMs"),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
):
    session = SessionLocal()
    try:
        def apply_coarse_filters(q):
            if not include_inactive:
                q = q.filter(NotamRecord.is_active == True)
            if notam_category:
                q = q.filter(NotamRecord.notam_category == notam_category.value)
            if primary_category:
                q = q.filter(NotamRecord.primary_category == primary_category.value)
            if start_time_after:
                q = q.filter(NotamRecord.start_time >= _to_utc(start_time_after))
            if end_time_before:
                q = q.filter(or_(NotamRecord.end_time <= _to_utc(end_time_before),
                                 NotamRecord.end_time.is_(None)))
            return q

        ids_subq = (
            session.query(NotamRecord.id)
            .join(NotamRecord.airports)
            .filter(Airport.icao_code == airport.upper())
            .distinct(NotamRecord.id)
        )
        ids_subq = apply_coarse_filters(ids_subq)
        ids_subq = ids_subq.order_by(
            NotamRecord.id,
            NotamRecord.start_time.desc(),
            NotamRecord.issue_time.desc(),
        ).offset(offset).limit(limit * (3 if active_only else 1))

        id_rows = [r[0] for r in ids_subq.all()]
        if not id_rows:
            return []

        q = (
            session.query(NotamRecord)
            .filter(NotamRecord.id.in_(id_rows))
            .options(
                selectinload(NotamRecord.airports),
                selectinload(NotamRecord.operational_tags),
                selectinload(NotamRecord.runways),
                selectinload(NotamRecord.runway_conditions),
                selectinload(NotamRecord.flight_phase_links),
                selectinload(NotamRecord.wingspan_restriction),
                selectinload(NotamRecord.aircraft_size_links),
                selectinload(NotamRecord.aircraft_propulsion_links),
                selectinload(NotamRecord.obstacles),
            )
        )
        rows = q.all()

        if active_only:
            now_utc = datetime.now(timezone.utc)
            rows = [r for r in rows if _is_active_now(r, now_utc)]

        rows.sort(key=lambda r: (
            r.start_time or datetime.min.replace(tzinfo=timezone.utc),
            r.issue_time or datetime.min.replace(tzinfo=timezone.utc),
            r.id
        ), reverse=True)

        rows = rows[:limit]
        return [format_notam(n) for n in rows]
    finally:
        session.close()

@app.get("/briefing-from-input")
async def get_briefing_from_input(
    query: str,
    current_user: AuthUser = Depends(get_current_user)
):
    try:
        result = await briefing_chain(query)
        return {"briefing": result, "generated_for": current_user.email, "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Briefing generation failed: {str(e)}")



@app.get("/enums/primary-categories", response_model=List[str])
def list_primary_categories(current_user: AuthUser = Depends(get_current_user)):
    return [e.value for e in PrimaryCategoryEnum]

@app.get("/enums/notam-categories", response_model=List[str])
def list_notam_categories(current_user: AuthUser = Depends(get_current_user)):
    return [e.value for e in NotamCategoryEnum]

# -------------------- Optional Auth Routes --------------------
@app.get("/airports", response_model=List[dict])
def list_airports(
    current_user: Optional[AuthUser] = Depends(get_optional_user),
    limit: int = Query(100, ge=1, le=1000),
    search: Optional[str] = Query(None, description="Search by ICAO code or name")
):
    session = SessionLocal()
    try:
        query = session.query(Airport)
        if search:
            search_term = f"%{search.upper()}%"
            query = query.filter(or_(Airport.icao_code.ilike(search_term), Airport.name.ilike(search_term)))

        airports = query.limit(limit).all()

        result = []
        for airport in airports:
            airport_data = {
                "icao_code": airport.icao_code,
                "name": airport.name,
                "country": airport.country,
            }
            if current_user:
                airport_data.update({
                    "iata_code": airport.iata_code,
                    "coordinates": {
                        "lat": airport.lat,
                        "lon": airport.lon,
                        "elevation": airport.elev
                    } if airport.lat and airport.lon else None,
                    "timezone": airport.timezone,
                })
            result.append(airport_data)

        return result
    finally:
        session.close()

# -------------------- Error Handlers --------------------
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.now().isoformat(),
        },
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8080)
