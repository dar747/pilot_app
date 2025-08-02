import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from notam.db import NotamRecord
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Environment variables
LOCAL_DB_URL = os.getenv("LOCAL_DB_URL")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

# Create engines and sessions
local_engine = create_engine(LOCAL_DB_URL)
supabase_engine = create_engine(SUPABASE_DB_URL)

LocalSession = sessionmaker(bind=local_engine)
SupabaseSession = sessionmaker(bind=supabase_engine)

# Optional: Ensure Supabase table exists
# Base.metadata.create_all(supabase_engine)

def get_local_notams():
    session = LocalSession()
    records = session.query(NotamRecord).all()
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

def clear_supabase_table():
    session = SupabaseSession()
    try:
        deleted = session.query(NotamRecord).delete()
        session.commit()
        print(f"🧹 Cleared {deleted} NOTAM records from Supabase")
    except Exception as e:
        session.rollback()
        print(f"❌ Failed to clear Supabase: {e}")
    finally:
        session.close()


def push_new_to_supabase(overwrite=False):
    local_records = get_local_notams()

    if overwrite:
        clear_supabase_table()
        new_records = local_records  # Push everything
    else:
        supabase_hashes = get_supabase_hashes()
        new_records = [r for r in local_records if r.raw_hash not in supabase_hashes]

    print(f"🆕 Found {len(new_records)} new NOTAMs to push to Supabase")

    if not new_records:
        print("🎉 Nothing to upload.")
        return

    session = SupabaseSession()
    try:
        for record in new_records:
            # Create a fresh instance for Supabase
            new_record = NotamRecord(
                notam_number=record.notam_number,
                issue_time=record.issue_time,
                notam_info_type=record.notam_info_type,
                notam_category=record.notam_category,
                airport=record.airport,
                start_time=record.start_time,
                end_time=record.end_time,
                seriousness=record.seriousness,
                applied_scenario=record.applied_scenario,
                applied_aircraft_type=record.applied_aircraft_type,
                operational_tag=record.operational_tag,
                affected_runway=record.affected_runway,
                notam_summary=record.notam_summary,
                icao_message=record.icao_message,
                replacing_notam=record.replacing_notam,
                raw_hash=record.raw_hash
            )
            session.add(new_record)

        session.commit()
        print(f"✅ Uploaded {len(new_records)} NOTAMs to Supabase")
    except Exception as e:
        session.rollback()
        print(f"❌ Upload error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    push_new_to_supabase(overwrite=False)  # default: push only new

