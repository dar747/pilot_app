# notam/reset_supabase_schema.py
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from notam.db import Base

load_dotenv()

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
if not SUPABASE_DB_URL:
    raise ValueError("❌ SUPABASE_DB_URL is missing from .env")

engine = create_engine(SUPABASE_DB_URL)

def reset_supabase_schema():
    with engine.connect() as conn:
        print("⚠️ Dropping ALL tables (CASCADE) in Supabase…")
        conn.execute(text("DROP SCHEMA public CASCADE;"))
        conn.execute(text("CREATE SCHEMA public;"))
        conn.commit()
        print("✅ Schema dropped and recreated.")

    print("📦 Recreating tables from SQLAlchemy models…")
    Base.metadata.create_all(bind=engine)
    print("✅ Tables recreated successfully.")

if __name__ == "__main__":
    reset_supabase_schema()
