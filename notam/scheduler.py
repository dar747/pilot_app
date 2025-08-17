# notam/scheduler.py
import os
import json
import hashlib
import asyncio
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

import pandas as pd
import requests
from sqlalchemy.exc import IntegrityError

from notam.analyze import analyze_notam
from notam.scoring import compute_base_score  # NEW: scoring
from notam.db import (
    init_db, SessionLocal,
    # ORM models
    NotamRecord, Airport, OperationalTag, NotamHistory,
    NotamWingspanRestriction, NotamTaxiway, NotamProcedure, NotamObstacle,
    NotamRunway, NotamRunwayCondition, NotamFlightPhase,
    # Enums
    SeverityLevelEnum, TimeClassificationEnum, TimeOfDayApplicabilityEnum,
    FlightRuleApplicabilityEnum, AircraftSizeEnum, AircraftPropulsionEnum,
    PrimaryCategoryEnum, NotamCategoryEnum, FlightPhaseEnum,
    # M2M tables
    notam_aircraft_propulsions, notam_aircraft_sizes,  # NEW: sizes was missing
)

# ----------------- helpers -----------------

def to_utc_aware(dt_like):
    """str|datetime -> timezone-aware *UTC* datetime. Z/offset handled; naive assumed UTC."""
    if isinstance(dt_like, str):
        s = dt_like.strip()
        if s.endswith(('Z', 'z')):
            s = s[:-1] + '+00:00'
        dt = datetime.fromisoformat(s)  # OK for '...+08:00'
    elif isinstance(dt_like, datetime):
        dt = dt_like
    else:
        raise TypeError(f"Unsupported datetime type: {type(dt_like)!r}")

    if dt.tzinfo is None:                 # naive → assume UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)     # <-- normalize to UTC

def parse_runway_id(runway_id: str) -> Tuple[Optional[int], Optional[str]]:
    """'07L' -> (7,'L'); '18' -> (18, None)."""
    if not runway_id:
        return None, None
    s = runway_id.strip().upper()
    side = s[-1] if s[-1:] in {'L','C','R'} else None
    if side:
        s = s[:-1]
    try:
        num = int(s)
        return (num, side) if 1 <= num <= 36 else (None, side)
    except ValueError:
        return None, side

BASE_DIR = Path(__file__).resolve().parent.parent  # project root

os.environ["LANGCHAIN_TRACING_V2"] = "true"
langchain_api_key = os.getenv("LANGCHAIN_API_KEY")
os.environ["LANGCHAIN_PROJECT"] = "Analyse_NOTAM"

# ----------------- main pipeline -----------------

def build_and_populate_db(overwrite: bool = False):
    init_db()
    csv_path = BASE_DIR / "data" / "Airport Database - NOTAM ID.csv"
    all_notams = fetch_notam_data_from_csv(str(csv_path))

    if overwrite:
        clear_db()
        existing_hashes = set()
    else:
        existing_hashes = get_existing_hashes()

    to_analyze = []
    seen_in_run = set()

    for n in all_notams[0:10]:  # keep your local cap; remove for full run
        h = get_hash(n["notam_number"], n["icao_message"])
        print(f"NOTAM: {n['notam_number']}, Hash: {h}")
        if h in existing_hashes or h in seen_in_run:
            print(f"⏩ Already in DB or batch, skipping {n['notam_number']} | hash: {h}")
            continue
        n["raw_hash"] = h
        to_analyze.append(n)
        seen_in_run.add(h)
        print(f"🆕 Will analyze: {n['notam_number']} | hash: {h}")

    print(f"✅ {len(to_analyze)} new NOTAMs to analyze")

    if not to_analyze:
        print("🎉 All NOTAMs already analyzed and stored. Exiting.")
        return

    asyncio.run(run_analysis(to_analyze, batch_size=200, max_concurrency=8))

def fetch_notam_data_from_csv(csv_path: str) -> List[Dict]:
    df = pd.read_csv(csv_path, usecols=['Designator', 'URL'], nrows=10)
    #df = pd.read_csv(csv_path, usecols=['Designator', 'URL']) # remove nrows for full
    df = df.dropna(how='all', subset=['Designator', 'URL'])
    df = df[~(
        (df['Designator'].astype(str).str.strip() == '') &
        (df['URL'].astype(str).str.strip() == '')
    )].reset_index(drop=True)

    print(f"🔗 {len(df)} links to fetch…")
    notam_objs = []

    for _, row in df.iterrows():
        designator = str(row['Designator']).strip()
        url = str(row['URL']).strip()
        if url.lower() in ["", "nan"]:
            print(f"⚠️  Skipping {designator} due to missing URL")
            continue

        print(f"📡 Fetching {designator}: {url}")
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                timeout=20
            )
            if resp.status_code == 200:
                data = resp.json()
                for n in data.get("notams", []):
                    msg = n.get("icaoMessage")
                    num = n.get("notamNumber")
                    date = n.get("issueDate")
                    if msg and num and msg.strip():
                        notam_objs.append({
                            "issue_time": date,
                            "notam_number": num.strip(),
                            "icao_message": msg.strip(),
                            "airport": designator,
                            "url": url
                        })
                print(f"✅ Stored {len(data.get('notams', []))} NOTAMs")
            else:
                print(f"❗ HTTP {resp.status_code} for {designator}")
        except requests.RequestException as e:
            print(f"❌ Error fetching {designator}: {e}")

    return notam_objs

def get_hash(notam_number, icao_message):
    combined = f"{notam_number.strip()}|{icao_message.strip()}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()

def get_existing_hashes():
    session = SessionLocal()
    try:
        hashes = set(x[0] for x in session.query(NotamRecord.raw_hash).all() if x[0])
        print(f"🔎 DB contains {len(hashes)} existing NOTAM hashes.")
        return hashes
    finally:
        session.close()

def clear_db():
    session = SessionLocal()
    try:
        num_deleted = session.query(NotamRecord).delete()
        session.commit()
        print(f"🧹 Cleared {num_deleted} NOTAM records from the database.")
    except Exception as e:
        session.rollback()
        print(f"❌ Failed to clear database: {e}")
    finally:
        session.close()

# ----------------- persistence -----------------

def iso_utc_z(dt_like) -> str | None:
    if dt_like is None:
        return None
    if isinstance(dt_like, str):
        s = dt_like.strip()
        # accept Z / offsets / naive
        if s.endswith(('Z','z')):
            s = s[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None  # or raise
    elif isinstance(dt_like, datetime):
        dt = dt_like
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def _none_if_nullish(x):
    return None if (x is None or (isinstance(x, str) and x.strip().upper() in {"", "NULL", "NONE"})) else x


def save_to_db(result, raw_text, notam_number, raw_hash, airport_code):
    """
    Upsert a single analyzed NOTAM (by raw_hash).
    """
    session = SessionLocal()
    try:
        # Upsert by raw_hash (unique)
        notam = session.query(NotamRecord).filter_by(raw_hash=raw_hash).first()
        is_update = bool(notam)
        if not is_update:
            notam = NotamRecord(raw_hash=raw_hash)

        # Core fields / enums  (FIXED: all use Enum types)
        notam.notam_number = notam_number
        notam.issue_time = iso_utc_z(result.issue_time)
        notam.notam_category = NotamCategoryEnum(result.notam_category.value)
        notam.severity_level = SeverityLevelEnum(result.severity_level.value)
        notam.start_time = iso_utc_z(result.start_time)
        notam.end_time = iso_utc_z(_none_if_nullish(getattr(result, "end_time", None)))
        notam.time_classification = (
            TimeClassificationEnum(result.time_classification.value)
            if getattr(result, "time_classification", None) else None
        )
        notam.time_of_day_applicability = TimeOfDayApplicabilityEnum(result.time_of_day_applicability.value)
        notam.flight_rule_applicability = FlightRuleApplicabilityEnum(result.flight_rule_applicability.value)
        notam.primary_category = PrimaryCategoryEnum(result.primary_category.value)  # FIXED typo

        notam.affected_area = result.affected_area.model_dump(exclude_none=True) if result.affected_area else None
        notam.affected_airports_snapshot = result.affected_airports or []
        notam.notam_summary = result.notam_summary
        notam.icao_message = raw_text
        notam.replacing_notam = result.replacing_notam or None

        if not is_update:
            session.add(notam)
            session.flush()  # get notam.id

        # Airports M2M
        def get_or_create_airport(code: str) -> Airport:
            ap = session.query(Airport).filter_by(icao_code=code).first()
            if not ap:
                ap = Airport(icao_code=code, name=f"{code} Airport")
                session.add(ap); session.flush()
            return ap

        primary_ap = get_or_create_airport(airport_code)
        if is_update:
            notam.airports.clear()
        if primary_ap not in notam.airports:
            notam.airports.append(primary_ap)

        for icao in (result.affected_airports or []):
            if icao and icao != airport_code:
                ap = get_or_create_airport(icao)
                if ap not in notam.airports:
                    notam.airports.append(ap)

        # Operational tags
        if is_update:
            notam.operational_tags.clear()
        for tag_name in (result.operational_tag or []):
            tag = session.query(OperationalTag).filter_by(tag_name=tag_name).first()
            if not tag:
                tag = OperationalTag(tag_name=tag_name)
                session.add(tag); session.flush()
            if tag not in notam.operational_tags:
                notam.operational_tags.append(tag)

        # Flight phases (as enums)
        session.query(NotamFlightPhase).filter_by(notam_id=notam.id).delete()
        for p in (result.flight_phases or []):
            session.add(NotamFlightPhase(notam_id=notam.id, phase=FlightPhaseEnum(p.value)))

        # Aircraft applicability
        aa = result.aircraft_applicability
        session.execute(notam_aircraft_sizes.delete().where(notam_aircraft_sizes.c.notam_id == notam.id))
        session.execute(notam_aircraft_propulsions.delete().where(notam_aircraft_propulsions.c.notam_id == notam.id))
        ws_old = session.query(NotamWingspanRestriction).filter_by(notam_id=notam.id).first()
        if ws_old:
            session.delete(ws_old)

        for s in (aa.sizes or []):
            session.execute(
                notam_aircraft_sizes.insert().values(notam_id=notam.id, size=AircraftSizeEnum(s.value))
            )
        for pr in (aa.propulsion or []):
            session.execute(
                notam_aircraft_propulsions.insert().values(notam_id=notam.id, propulsion=AircraftPropulsionEnum(pr.value))
            )

        ws = getattr(aa, "wingspan_restriction", None)
        if ws and any(v is not None for v in (ws.min_m, ws.max_m)):
            session.add(NotamWingspanRestriction(
                notam_id=notam.id,
                min_m=ws.min_m, min_inclusive=ws.min_inclusive,
                max_m=ws.max_m, max_inclusive=ws.max_inclusive
            ))

        # Extracted elements
        ee = getattr(result, "extracted_elements", None)

        session.query(NotamTaxiway).filter_by(notam_id=notam.id).delete()
        session.query(NotamProcedure).filter_by(notam_id=notam.id).delete()
        if ee:
            for t in (ee.taxiways or []):
                if t:
                    session.add(NotamTaxiway(
                        notam_id=notam.id, airport_code=primary_ap.icao_code, taxiway_id=str(t).upper()
                    ))
            for pr in (ee.procedures or []):
                if pr:
                    session.add(NotamProcedure(
                        notam_id=notam.id, airport_code=primary_ap.icao_code, procedure_name=str(pr).upper()
                    ))

        session.query(NotamObstacle).filter_by(notam_id=notam.id).delete()
        if ee:
            for o in (ee.obstacles or []):
                rp = getattr(o, "runway_reference", None)
                session.add(NotamObstacle(
                    notam_id=notam.id,
                    type=o.type,
                    height_agl_ft=o.height_agl_ft,
                    height_amsl_ft=o.height_amsl_ft,
                    latitude=(o.location.latitude if o.location else None),
                    longitude=(o.location.longitude if o.location else None),
                    lighting=o.lighting,
                    runway_id=(rp.runway_id if rp else None),
                    reference_type=(rp.reference_type if rp else None),
                    offset_distance_m=(rp.offset_distance_m if rp else None),
                    offset_direction=(rp.offset_direction if rp else None),
                    lateral_half_width_m=(rp.lateral_half_width_m if rp else None),
                    corridor_orientation=(rp.corridor_orientation if rp else None),
                ))

        session.query(NotamRunway).filter_by(notam_id=notam.id, airport_code=primary_ap.icao_code).delete()
        if ee:
            for rwy in (ee.runways or []):
                num, side = parse_runway_id(str(rwy))
                if num is not None:
                    session.add(NotamRunway(
                        notam_id=notam.id,
                        airport_code=primary_ap.icao_code,
                        runway_number=num,
                        runway_side=side
                    ))

        session.query(NotamRunwayCondition).filter_by(notam_id=notam.id).delete()
        if ee:
            for rc in (ee.runway_conditions or []):
                num, side = parse_runway_id(rc.runway_id)
                if num is not None:
                    session.add(NotamRunwayCondition(
                        notam_id=notam.id,
                        airport_code=primary_ap.icao_code,
                        runway_number=num,
                        runway_side=side,
                        friction_value=rc.friction_value
                    ))

        # Base score (NEW)
        score, features, why = compute_base_score(notam)
        notam.base_score = score
        notam.score_features = features
        notam.score_explanation = why

        # History
        session.add(NotamHistory(
            notam_id=notam.id,
            action=('UPDATED' if is_update else 'CREATED'),
            changed_fields={'updated_at': datetime.now(timezone.utc).isoformat()} if is_update else {}
        ))

        session.commit()
        print(f"📝 {'Updated' if is_update else 'Saved'} {notam_number} at {airport_code}")
        return notam.id

    except IntegrityError as e:
        session.rollback(); print(f"⚠️ Integrity error: {e}"); return None
    except Exception as e:
        session.rollback(); import traceback; traceback.print_exc(); print(f"❌ DB error: {e}"); return None
    finally:
        session.close()

# ----------------- batch/async orchestration -----------------

async def run_analysis(to_analyze: List[Dict], batch_size=100, max_concurrency=8):
    print(f"📦 Running analysis on {len(to_analyze)} new NOTAMs...")

    sem = asyncio.Semaphore(max_concurrency)

    async def _analyze_one(n: Dict):
        async with sem:
            return await analyze_notam(n["icao_message"], n["issue_time"])

    for i in range(0, len(to_analyze), batch_size):
        batch = to_analyze[i:i+batch_size]
        tasks = [_analyze_one(n) for n in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for j, result in enumerate(results):
            notam_number = batch[j]["notam_number"]
            icao_message = batch[j]["icao_message"]
            airport = batch[j].get("airport", "Unknown")
            url = batch[j].get("url", "Unknown")
            raw_hash = batch[j].get("raw_hash", "Unknown")

            if isinstance(result, Exception) or result is None:
                print(f"❌ Error analyzing NOTAM {notam_number}: {result}")
                continue

            print(f"\n--- ICAO Message for NOTAM {notam_number} ---\n{icao_message}\n")
            print(f"🔗 Source URL: {url}")
            print(f"📊 Analysis Result for {notam_number} ({airport}):\n{json.dumps(result.model_dump(), indent=2)}\n")

            save_to_db(
                result=result,
                raw_text=icao_message,
                notam_number=notam_number,
                raw_hash=raw_hash,
                airport_code=airport
            )

# ----------------- entrypoint -----------------

if __name__ == "__main__":
    build_and_populate_db(overwrite=True)
