import pandas as pd
import requests
import asyncio
import hashlib
from typing import List, Dict

from dotenv.main import rewrite

from notam.analyze import analyze_notam
from notam.db import NotamRecord, SessionLocal, init_db
from notam.db import (
    NotamRecord, Airport, OperationalTag, FilterTag, NotamAcknowledgment,
    SessionLocal, init_db, NotamHistory, notam_runways
)

import json
from pathlib import Path

from datetime import datetime
from sqlalchemy.exc import IntegrityError
import json
import hashlib
import os

BASE_DIR = Path(__file__).resolve().parent.parent  # Goes up from notam/ to project root


os.environ["LANGCHAIN_TRACING_V2"] = "false"
os.environ["LANGCHAIN_API_KEY"] = "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"
os.environ["LANGCHAIN_PROJECT"] = "PILOT"


def build_and_populate_db(overwrite=False):
    init_db()
    csv_path = BASE_DIR / "data" / "Airport Database - NOTAM ID.csv"
    all_notams = fetch_notam_data_from_csv(str(csv_path))
    if overwrite:
        clear_db()
        existing_hashes = set()
    else:
        existing_hashes = get_existing_hashes()

    to_analyze = []
    seen_in_run = set()  # NEW

    for n in all_notams:
        h = get_hash(n["notam_number"], n["icao_message"])
        print(f"NOTAM: {n['notam_number']}, Hash: {h}")  # <-- This line prints the hash
        if h in existing_hashes or h in seen_in_run:  # MODIFIED
            print(f"⏩ Already in DB or batch, skipping {n['notam_number']} | hash: {h}")
        else:
            n["raw_hash"] = h
            to_analyze.append(n)
            seen_in_run.add(h)  # NEW
            print(f"🆕 Will analyze: {n['notam_number']} | hash: {h}")

    print(f"✅ {len(to_analyze)} new NOTAMs to analyze")

    if not to_analyze:
        print("🎉 All NOTAMs already analyzed and stored. Exiting.")
    else:
        asyncio.run(run_analysis(to_analyze, batch_size=800))



def fetch_notam_data_from_csv(csv_path: str) -> List[Dict]:
    #df = pd.read_csv(csv_path, usecols=['Designator', 'URL'], nrows=10)
    df = pd.read_csv(csv_path, usecols=['Designator', 'URL'])
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
                timeout=10
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
                            "url":url
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
    hashes = set(x[0] for x in session.query(NotamRecord.raw_hash).all() if x[0])
    session.close()
    print(f"🔎 DB contains {len(hashes)} existing NOTAM hashes.")
    return hashes

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


# def save_to_db(result, raw_text, notam_number, raw_hash, airport):
#     session = SessionLocal()
#     try:
#         exists = session.query(NotamRecord).filter(
#             (NotamRecord.airport == airport) &
#             (NotamRecord.notam_number == notam_number)
#         ).first()
#
#         if exists:
#             print(f"⏩ Skipping duplicate NOTAM {notam_number} at {airport}")
#             return
#
#         record = NotamRecord(
#             airport=airport,
#             notam_number=notam_number,
#             issue_time=result.issue_time,
#             notam_info_type=result.notam_info_type,
#             notam_category=result.notam_category,
#             start_time=result.start_time,
#             end_time=result.end_time,
#             seriousness=result.seriousness,
#             applied_scenario=result.applied_scenario,
#             applied_aircraft_type=result.applied_aircraft_type,
#             operational_tag=",".join(result.operational_tag) if isinstance(result.operational_tag, list) else result.operational_tag,
#             affected_runway=",".join(result.affected_runway) if isinstance(result.affected_runway, list) else result.affected_runway,
#             notam_summary=result.notam_summary,
#             icao_message=raw_text,
#             replacing_notam=result.replacing_notam,
#             raw_hash=raw_hash
#         )
#         session.add(record)
#         session.commit()
#         print(f"📝 Saved {result.notam_number} at {airport}")
#     except Exception as e:
#         session.rollback()
#         print(f"❌ DB error: {e}")
#     finally:
#         session.close()

def save_to_db(result, raw_text, notam_number, raw_hash, airport):
    """
    Save enhanced NOTAM analysis result to database with all relationships
    """
    session = SessionLocal()
    try:
        # Check for existing NOTAM using unique constraint
        exists = session.query(NotamRecord).filter(
            NotamRecord.raw_hash == raw_hash
        ).first()

        if exists:
            print(f"⏩ Updating existing NOTAM {notam_number}")
            notam_record = exists
            is_update = True
        else:
            notam_record = NotamRecord()
            is_update = False

        # Convert datetime strings to datetime objects
        issue_time = datetime.fromisoformat(result.issue_time.replace('Z', '+00:00'))
        start_time = datetime.fromisoformat(result.start_time.replace('Z', '+00:00'))
        end_time = None
        if result.end_time and result.end_time.lower() not in ['none', 'null', 'perm']:
            try:
                end_time = datetime.fromisoformat(result.end_time.replace('Z', '+00:00'))
            except:
                pass

        # Basic fields
        notam_record.notam_number = notam_number
        notam_record.issue_time = issue_time
        notam_record.notam_info_type = result.notam_info_type
        notam_record.notam_category = result.notam_category

        # Enhanced severity classification
        notam_record.seriousness = result.seriousness  # Legacy field
        notam_record.severity_level = result.severity_level
        notam_record.urgency_indicator = result.urgency_indicator

        # Temporal information
        notam_record.start_time = start_time
        notam_record.end_time = end_time
        notam_record.time_classification = result.time_classification
        notam_record.schedule = getattr(result, 'schedule', None)

        # Applicability
        notam_record.applied_scenario = result.applied_scenario
        notam_record.applied_aircraft_type = result.applied_aircraft_type
        notam_record.aircraft_categories = result.aircraft_categories
        notam_record.flight_phases = result.flight_phases

        # Categorization
        notam_record.primary_category = result.primary_category
        notam_record.secondary_categories = result.secondary_categories

        # Location information
        notam_record.affected_fir = getattr(result, 'affected_fir', None)
        notam_record.affected_coordinate = getattr(result, 'affected_coordinate', None)

        # Convert affected_area to dict if it exists
        if hasattr(result, 'affected_area') and result.affected_area:
            notam_record.affected_area = result.affected_area.dict()

        # Content
        notam_record.notam_summary = result.notam_summary
        notam_record.icao_message = raw_text
        notam_record.raw_text = getattr(result, 'raw_text', raw_text)

        # Complex structures as JSON
        if hasattr(result, 'extracted_elements'):
            notam_record.extracted_elements = result.extracted_elements.dict()

        if hasattr(result, 'operational_impact'):
            notam_record.operational_impact = result.operational_impact.dict()

        if hasattr(result, 'safety_assessment'):
            notam_record.safety_assessment = result.safety_assessment.dict()

        # Administrative
        notam_record.replacing_notam = result.replacing_notam if result.replacing_notam != 'None' else None
        notam_record.replaced_by = getattr(result, 'replaced_by', None)
        notam_record.related_notams = getattr(result, 'related_notams', [])

        # Multi-category support
        notam_record.multi_category_rationale = getattr(result, 'multi_category_rationale', None)

        # App-specific fields
        notam_record.requires_acknowledgment = getattr(result, 'requires_acknowledgment', False)
        notam_record.display_priority = result.display_priority

        # Validation and quality
        notam_record.confidence_score = result.confidence_score
        notam_record.validation_warnings = getattr(result, 'validation_warnings', [])

        # Tracking
        notam_record.raw_hash = raw_hash

        # Add to session if new
        if not is_update:
            session.add(notam_record)
            session.flush()  # Get the ID for relationships

        # Handle airport relationships
        # First, ensure the airport exists in the airports table
        airport_record = session.query(Airport).filter_by(icao_code=airport).first()
        if not airport_record:
            airport_record = Airport(
                icao_code=airport,
                # You might want to fetch these from another source
                name=f"{airport} Airport",
            )
            session.add(airport_record)
            session.flush()

        # Clear existing airport relationships if updating
        if is_update:
            notam_record.airports = []

        # Add primary airport
        if airport_record not in notam_record.airports:
            notam_record.airports.append(airport_record)

        # Add any additional affected airports
        if hasattr(result, 'affected_airports'):
            for icao in result.affected_airports:
                if icao != airport:  # Don't duplicate primary airport
                    airport_rec = session.query(Airport).filter_by(icao_code=icao).first()
                    if not airport_rec:
                        airport_rec = Airport(icao_code=icao, name=f"{icao} Airport")
                        session.add(airport_rec)
                    if airport_rec not in notam_record.airports:
                        notam_record.airports.append(airport_rec)

        # Handle operational tags
        if is_update:
            notam_record.operational_tags = []

        for tag_name in result.operational_tag:
            tag = session.query(OperationalTag).filter_by(tag_name=tag_name).first()
            if not tag:
                # Determine if tag is critical based on name
                is_critical = any(critical in tag_name.lower() for critical in
                                  ['closure', 'u/s', 'outage', 'failure', 'emergency'])
                tag = OperationalTag(
                    tag_name=tag_name,
                    is_critical=is_critical
                )
                session.add(tag)
                session.flush()
            if tag not in notam_record.operational_tags:
                notam_record.operational_tags.append(tag)

        # Handle filter tags
        if is_update:
            notam_record.filter_tags = []

        if hasattr(result, 'filter_tags'):
            for tag_name in result.filter_tags:
                tag = session.query(FilterTag).filter_by(tag_name=tag_name).first()
                if not tag:
                    tag = FilterTag(tag_name=tag_name)
                    session.add(tag)
                    session.flush()
                if tag not in notam_record.filter_tags:
                    notam_record.filter_tags.append(tag)

        # Handle runway relationships (stored in association table)
        # First clear existing if updating
        if is_update:
            session.execute(
                notam_runways.delete().where(notam_runways.c.notam_id == notam_record.id)
            )

        # Add runway relationships
        if result.affected_runway and result.affected_runway[0] != 'None':
            for runway in result.affected_runway:
                if runway and runway != 'None':
                    session.execute(
                        notam_runways.insert().values(
                            notam_id=notam_record.id,
                            runway_id=runway
                        )
                    )

        # Create history entry
        if is_update:
            history = NotamHistory(
                notam_id=notam_record.id,
                action='UPDATED',
                changed_fields={'updated_at': datetime.utcnow().isoformat()}
            )
        else:
            history = NotamHistory(
                notam_id=notam_record.id,
                action='CREATED',
                changed_fields={}
            )
        session.add(history)

        # Commit all changes
        session.commit()

        action = "Updated" if is_update else "Saved"
        print(f"📝 {action} {result.notam_number} at {airport} with priority {result.display_priority}")

        # Log tag associations
        print(f"   - Operational tags: {', '.join(result.operational_tag)}")
        if hasattr(result, 'filter_tags'):
            print(f"   - Filter tags: {len(result.filter_tags)} tags")

        return notam_record.id

    except IntegrityError as e:
        session.rollback()
        print(f"⚠️ Integrity error (possible duplicate): {e}")
        # Try to update instead
        return None
    except Exception as e:
        session.rollback()
        print(f"❌ DB error: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        session.close()


def batch_save_notams(notam_results, airport_code):
    """
    Save multiple NOTAMs efficiently in a batch
    """
    session = SessionLocal()
    saved_count = 0
    updated_count = 0

    try:
        # Pre-fetch all airports and tags to avoid repeated queries
        airport = session.query(Airport).filter_by(icao_code=airport_code).first()
        if not airport:
            airport = Airport(icao_code=airport_code, name=f"{airport_code} Airport")
            session.add(airport)
            session.flush()

        # Process each NOTAM
        for result, raw_text, notam_number, raw_hash in notam_results:
            try:
                # Similar logic as save_to_db but optimized for batch
                exists = session.query(NotamRecord).filter(
                    NotamRecord.notam_number == notam_number,
                    NotamRecord.raw_hash == raw_hash
                ).first()

                if exists:
                    updated_count += 1
                    continue

                # Create new record (similar to save_to_db logic)
                # ... (implement batch-optimized version)

                saved_count += 1

            except Exception as e:
                print(f"Error processing NOTAM {notam_number}: {e}")
                continue

        session.commit()
        print(f"✅ Batch save complete: {saved_count} new, {updated_count} existing")

    except Exception as e:
        session.rollback()
        print(f"❌ Batch save error: {e}")
    finally:
        session.close()

#
# def get_or_create_airport(session, icao_code, airport_data=None):
#     """
#     Helper function to get or create an airport record
#     """
#     airport = session.query(Airport).filter_by(icao_code=icao_code).first()
#     if not airport:
#         airport = Airport(
#             icao_code=icao_code,
#             name=airport_data.get('name', f"{icao_code} Airport") if airport_data else f"{icao_code} Airport",
#             city=airport_data.get('city') if airport_data else None,
#             country=airport_data.get('country') if airport_data else None,
#             latitude=airport_data.get('latitude') if airport_data else None,
#             longitude=airport_data.get('longitude') if airport_data else None,
#             elevation_ft=airport_data.get('elevation_ft') if airport_data else None
#         )
#         session.add(airport)
#         session.flush()
#     return airport



async def run_analysis(to_analyze: List[Dict], batch_size=100):
    print(f"📦 Running analysis on {len(to_analyze)} new NOTAMs...")
    for i in range(0, len(to_analyze), batch_size):
        batch = to_analyze[i:i+batch_size]
        tasks = [analyze_notam(n["icao_message"],n["issue_time"]) for n in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for j, result in enumerate(results):
            notam_number = batch[j]["notam_number"]
            icao_message = batch[j]["icao_message"]
            airport = batch[j].get("airport", "Unknown")
            url = batch[j].get("url", "Unknown")
            if isinstance(result, Exception) or result is None:
                print(f"❌ Error analyzing NOTAM {notam_number}: {result}")
            else:
                print(f"\n--- ICAO Message for NOTAM {notam_number} ---\n{icao_message}\n")
                print(f"🔗 Source URL: {url}")
                print(f"📊 Analysis Result for {notam_number} ({airport}):\n{json.dumps(result.model_dump(), indent=2)}\n")

                save_to_db(
                    result,
                    icao_message,
                    notam_number,
                    batch[j]["raw_hash"],
                    airport
                )


if __name__ == "__main__":
    build_and_populate_db()



