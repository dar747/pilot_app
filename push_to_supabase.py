import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db import NotamRecord, Base
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Connect to local and Supabase databases
LOCAL_DB_URL = os.getenv("LOCAL_DB_URL")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

local_engine = create_engine(LOCAL_DB_URL)
supabase_engine = create_engine(SUPABASE_DB_URL)

LocalSession = sessionmaker(bind=local_engine)
SupabaseSession = sessionmaker(bind=supabase_engine)

def get_local_notams():
    session = LocalSession()
    records = session.query(NotamRecord).all()
    session.close()
    return records

def get_supabase_hashes():
    session = SupabaseSession()
    hashes = set(x[0] for x in session.query(NotamRecord.raw_hash).all() if x[0])
    session.close()
    return hashes

def push_new_to_supabase():
    local_records = get_local_notams()
    supabase_hashes = get_supabase_hashes()

    new_records = [r for r in local_records if r.raw_hash not in supabase_hashes]
    print(f"🆕 Found {len(new_records)} new NOTAMs to push to Supabase")

    if not new_records:
        print("🎉 Nothing to upload.")
        return

    session = SupabaseSession()
    try:
        for record in new_records:
            session.add(record)
        session.commit()
        print(f"✅ Uploaded {len(new_records)} NOTAMs to Supabase")
    except Exception as e:
        session.rollback()
        print(f"❌ Upload error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    push_new_to_supabase()
