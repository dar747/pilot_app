from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")  # Or hardcode for test

engine = create_engine(SUPABASE_DB_URL)

try:
    with engine.connect() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM notams"))
        count = result.scalar()
        print(f"✅ Connected! Total NOTAMs: {count}")
except Exception as e:
    print(f"❌ Connection failed: {e}")
