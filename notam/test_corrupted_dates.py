# test_corrupted_dates.py
from dotenv import load_dotenv
from sqlalchemy import text, create_engine
import os

load_dotenv()
engine = create_engine(os.getenv("LOCAL_DB_URL"))

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT COUNT(*) as total,
               COUNT(CASE WHEN extract(year from start_time) > 2100 OR extract(year from start_time) < 1900 THEN 1 END) as bad_start,
               COUNT(CASE WHEN extract(year from end_time) > 2100 OR extract(year from end_time) < 1900 THEN 1 END) as bad_end,
               COUNT(CASE WHEN extract(year from issue_time) > 2100 OR extract(year from issue_time) < 1900 THEN 1 END) as bad_issue
        FROM notams
    """))

    row = result.fetchone()
    print(f"📊 Database stats:")
    print(f"  Total NOTAMs: {row[0]}")
    print(f"  Bad start_time: {row[1]}")
    print(f"  Bad end_time: {row[2]}")
    print(f"  Bad issue_time: {row[3]}")