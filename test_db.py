from db import SessionLocal, NotamRecord, DATABASE_URL

print(f"Using DB: {DATABASE_URL}")

session = SessionLocal()
records = session.query(NotamRecord).all()
session.close()

print(f"Found {len(records)} NOTAMs")
