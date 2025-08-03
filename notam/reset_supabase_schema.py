# reset_supabase_schema.py
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from notam.db import Base  # import Base from your models

load_dotenv()

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
if not SUPABASE_DB_URL:
    raise ValueError("❌ SUPABASE_DB_URL is missing from .env")

engine = create_engine(SUPABASE_DB_URL)

def reset_supabase_schema():
    print("⚠️ Dropping all tables in Supabase...")
    Base.metadata.drop_all(bind=engine)
    print("✅ All tables dropped.")

    print("📦 Recreating tables from SQLAlchemy models...")
    Base.metadata.create_all(bind=engine)
    print("✅ Tables recreated successfully.")

if __name__ == "__main__":
    reset_supabase_schema()
