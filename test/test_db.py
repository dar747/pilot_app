import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from notam.db import init_db, SessionLocal, NotamRecord

init_db()
print("✅ Database and tables created successfully!")

# Count NOTAM records
session = SessionLocal()
count = session.query(NotamRecord).count()
print(f"🔢 Total NOTAM records: {count}")
session.close()
