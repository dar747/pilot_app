import os
from sqlalchemy import create_engine, select, insert
from sqlalchemy.orm import sessionmaker, joinedload
from dotenv import load_dotenv

from notam.db import (
    Base, NotamRecord, Airport, OperationalTag, FilterTag,
    notam_operational_tags, notam_filter_tags  # <-- Ensure both association tables are imported
)

load_dotenv()

LOCAL_DB_URL = os.getenv("LOCAL_DB_URL")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

if not SUPABASE_DB_URL:
    raise ValueError("❌ SUPABASE_DB_URL is missing from .env")

# Set up DB engines and sessions
local_engine = create_engine(LOCAL_DB_URL)
supabase_engine = create_engine(SUPABASE_DB_URL)
LocalSession = sessionmaker(bind=local_engine)
SupabaseSession = sessionmaker(bind=supabase_engine)

def get_local_notams():
    session = LocalSession()
    records = session.query(NotamRecord).options(
        joinedload(NotamRecord.airports),
        joinedload(NotamRecord.operational_tags),
        joinedload(NotamRecord.filter_tags)
    ).all()
    session.close()
    return records

def get_supabase_hashes():
    session = SupabaseSession()
    try:
        hashes = set(x[0] for x in session.query(NotamRecord.raw_hash).all() if x[0])
    except Exception as e:
        print(f"❌ Error reading Supabase hashes: {e}")
        hashes = set()
    finally:
        session.close()
    return hashes

def clear_supabase():
    session = SupabaseSession()
    try:
        print("⚠️ Dropping all data from Supabase tables...")
        for tbl in reversed(Base.metadata.sorted_tables):
            session.execute(tbl.delete())
        session.commit()
        print("🧹 Supabase cleared.")
    except Exception as e:
        session.rollback()
        print(f"❌ Failed to clear Supabase: {e}")
    finally:
        session.close()

def push_to_supabase(overwrite=False):
    local_records = get_local_notams()
    if overwrite:
        clear_supabase()
        records_to_push = local_records
    else:
        existing_hashes = get_supabase_hashes()
        records_to_push = [r for r in local_records if r.raw_hash not in existing_hashes]

    print(f"📦 Ready to push {len(records_to_push)} NOTAMs to Supabase")

    if not records_to_push:
        print("✅ Nothing to push. Supabase is up to date.")
        return

    session = SupabaseSession()

    try:
        for record in records_to_push:
            # Create new NOTAM record
            new = NotamRecord(
                notam_number=record.notam_number,
                issue_time=record.issue_time,
                notam_info_type=record.notam_info_type,
                notam_category=record.notam_category,
                start_time=record.start_time,
                end_time=record.end_time,
                seriousness=record.seriousness,
                severity_level=record.severity_level,
                urgency_indicator=record.urgency_indicator,
                applied_scenario=record.applied_scenario,
                applied_aircraft_type=record.applied_aircraft_type,
                aircraft_categories=record.aircraft_categories,
                flight_phases=record.flight_phases,
                primary_category=record.primary_category,
                secondary_categories=record.secondary_categories,
                affected_fir=record.affected_fir,
                affected_coordinate=record.affected_coordinate,
                affected_area=record.affected_area,
                extracted_elements=record.extracted_elements,
                notam_summary=record.notam_summary,
                operational_impact=record.operational_impact,
                safety_assessment=record.safety_assessment,
                replacing_notam=record.replacing_notam,
                replaced_by=record.replaced_by,
                related_notams=record.related_notams,
                multi_category_rationale=record.multi_category_rationale,
                requires_acknowledgment=record.requires_acknowledgment,
                display_priority=record.display_priority,
                raw_hash=record.raw_hash,
                confidence_score=record.confidence_score,
                validation_warnings=record.validation_warnings,
                raw_text=record.raw_text,
                icao_message=record.icao_message
            )

            session.add(new)
            session.flush()

            # 🔗 Airports
            for a in record.airports:
                airport = session.query(Airport).filter_by(icao_code=a.icao_code).first()
                if not airport:
                    airport = Airport(icao_code=a.icao_code, name=a.name or f"{a.icao_code} Airport")
                    session.add(airport)
                    session.flush()
                new.airports.append(airport)

            # 🔗 Operational Tags (with airport_code)
            for a in record.airports:
                for t in record.operational_tags:
                    tag = session.query(OperationalTag).filter_by(tag_name=t.tag_name).first()
                    if not tag:
                        tag = OperationalTag(tag_name=t.tag_name, is_critical=t.is_critical)
                        session.add(tag)
                        session.flush()

                    exists = session.execute(
                        select(notam_operational_tags).where(
                            notam_operational_tags.c.notam_id == new.id,
                            notam_operational_tags.c.tag_id == tag.id,
                            notam_operational_tags.c.airport_code == a.icao_code
                        )
                    ).first()

                    if not exists:
                        session.execute(
                            insert(notam_operational_tags).values(
                                notam_id=new.id,
                                tag_id=tag.id,
                                airport_code=a.icao_code
                            )
                        )

            # 🔗 Filter Tags (optional: if using airport_code)
            for a in record.airports:
                for t in record.filter_tags:
                    tag = session.query(FilterTag).filter_by(tag_name=t.tag_name).first()
                    if not tag:
                        tag = FilterTag(tag_name=t.tag_name)
                        session.add(tag)
                        session.flush()

                    exists = session.execute(
                        select(notam_filter_tags).where(
                            notam_filter_tags.c.notam_id == new.id,
                            notam_filter_tags.c.tag_id == tag.id,
                            notam_filter_tags.c.airport_code == a.icao_code
                        )
                    ).first()

                    if not exists:
                        session.execute(
                            insert(notam_filter_tags).values(
                                notam_id=new.id,
                                tag_id=tag.id,
                                airport_code=a.icao_code
                            )
                        )

        session.commit()
        print(f"✅ Successfully pushed {len(records_to_push)} NOTAMs to Supabase.")

    except Exception as e:
        session.rollback()
        print(f"❌ Push error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        session.close()

if __name__ == "__main__":
    push_to_supabase(overwrite=True)  # Change to False for incremental sync
