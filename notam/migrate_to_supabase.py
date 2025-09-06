# notam/migrate_to_supabase.py
import os
from dotenv import load_dotenv
from sqlalchemy import text  # <- This import was missing!


def migrate_schema():
    """Migrate your schema to Supabase"""
    load_dotenv()

    print("🚀 Starting Supabase migration...")

    # Import your existing modules
    from notam.db import init_db, engine, SessionLocal, NotamRecord

    try:
        # Test connection first
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))  # <- Fixed: wrapped in text()
            version = result.scalar()
            print(f"✅ Connected to: {version}")

        # Create all tables
        print("🔨 Creating database schema...")
        init_db()  # This calls Base.metadata.create_all(bind=engine)

        # Verify tables were created
        session = SessionLocal()
        try:
            count = session.query(NotamRecord).count()
            print(f"📊 NOTAMs table ready (current count: {count})")
        finally:
            session.close()

        print("🎉 Migration completed successfully!")

    except Exception as e:
        print(f"❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


if __name__ == "__main__":
    success = migrate_schema()
    exit(0 if success else 1)