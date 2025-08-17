# push_to_supabase.py
import os
import sys
from contextlib import contextmanager

from sqlalchemy import create_engine, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, joinedload
from dotenv import load_dotenv   # <-- add

from notam.db import (
    Base, NotamRecord, Airport, OperationalTag,
    notam_operational_tags,
)

# ---------- env / engines ----------

load_dotenv()  # <-- add: load .env before getenv

LOCAL_DB_URL = os.getenv("LOCAL_DB_URL")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

if not LOCAL_DB_URL:
    print("❌ LOCAL_DB_URL missing.")
    sys.exit(1)
if not SUPABASE_DB_URL:
    print("❌ SUPABASE_DB_URL missing.")
    sys.exit(1)

def ensure_sslmode_require(url: str) -> str:
    if "sslmode=" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}sslmode=require"

SUPABASE_DB_URL = ensure_sslmode_require(SUPABASE_DB_URL)

local_engine = create_engine(LOCAL_DB_URL, pool_pre_ping=True, future=True)
supabase_engine = create_engine(SUPABASE_DB_URL, pool_pre_ping=True, future=True)

# 👇 prevent attribute expiration on commit for LOCAL reads
LocalSession = sessionmaker(bind=local_engine, future=True, expire_on_commit=False)
# remote session can stay default; we commit writes there
SupabaseSession = sessionmaker(bind=supabase_engine, future=True)

@contextmanager
def local_session():
    s = LocalSession()
    try:
        yield s
        # no commit needed for read-only; but harmless to leave
        # s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()

@contextmanager
def remote_session():
    s = SupabaseSession()
    try:
        yield s
        s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()

# ---------- helpers ----------

def get_local_notams():
    # joinedload eager-loads relationships so they’re available after session closes
    with local_session() as s:
        return (
            s.query(NotamRecord)
            .options(
                joinedload(NotamRecord.airports),
                joinedload(NotamRecord.operational_tags),
            )
            .all()
        )

def get_supabase_hashes():
    with remote_session() as s:
        try:
            return {h for (h,) in s.query(NotamRecord.raw_hash).all() if h}
        except Exception as e:
            print(f"❌ Error reading Supabase hashes: {e}")
            return set()

def ensure_remote_schema():
    Base.metadata.create_all(bind=supabase_engine)

def clear_supabase():
    with remote_session() as s:
        print("⚠️  Deleting all data from Supabase tables (respecting FKs)…")
        for tbl in reversed(Base.metadata.sorted_tables):
            s.execute(tbl.delete())
        print("🧹 Supabase cleared.")

# ---------- core push ----------

def push_to_supabase(overwrite=False):
    ensure_remote_schema()

    local_records = get_local_notams()

    if overwrite:
        clear_supabase()
        records_to_push = local_records
    else:
        existing_hashes = get_supabase_hashes()
        records_to_push = [r for r in local_records if r.raw_hash not in existing_hashes]

    print(f"📦 Ready to push {len(records_to_push)} NOTAM(s) to Supabase")
    if not records_to_push:
        print("✅ Nothing to push. Supabase is up to date.")
        return

    with remote_session() as s:
        existing_airports = {a.icao_code: a for a in s.query(Airport).all()}
        existing_op_tags = {t.tag_name: t for t in s.query(OperationalTag).all()}

        op_links = set()
        pushed = 0

        for src in records_to_push:
            try:
                new = NotamRecord(
                    notam_number=src.notam_number,
                    issue_time=src.issue_time,
                    notam_category=src.notam_category,
                    severity_level=src.severity_level,
                    start_time=src.start_time,
                    end_time=src.end_time,
                    time_classification=src.time_classification,
                    time_of_day_applicability=src.time_of_day_applicability,
                    flight_rule_applicability=src.flight_rule_applicability,
                    primary_category=src.primary_category,
                    affected_area=src.affected_area,
                    affected_airports_snapshot=src.affected_airports_snapshot,
                    notam_summary=src.notam_summary,
                    icao_message=src.icao_message,
                    replacing_notam=src.replacing_notam,
                    raw_hash=src.raw_hash,
                    base_score=src.base_score,
                    score_features=src.score_features,
                    score_explanation=src.score_explanation,
                )
                s.add(new)
                s.flush()

                for a in src.airports:
                    ap = existing_airports.get(a.icao_code)
                    if not ap:
                        ap = Airport(icao_code=a.icao_code, name=a.name or f"{a.icao_code} Airport")
                        s.add(ap); s.flush()
                        existing_airports[a.icao_code] = ap
                    if ap not in new.airports:
                        new.airports.append(ap)

                for t in src.operational_tags:
                    tag = existing_op_tags.get(t.tag_name)
                    if not tag:
                        tag = OperationalTag(tag_name=t.tag_name)
                        s.add(tag); s.flush()
                        existing_op_tags[t.tag_name] = tag

                    key = (new.id, tag.id)
                    if key not in op_links:
                        try:
                            s.execute(insert(notam_operational_tags).values(
                                notam_id=new.id, tag_id=tag.id
                            ))
                            op_links.add(key)
                        except IntegrityError:
                            s.rollback()  # already exists; continue

                pushed += 1

            except IntegrityError as ie:
                s.rollback()
                print(f"⚠️  Skipping NOTAM {src.notam_number} (hash={src.raw_hash}) due to IntegrityError: {ie}")
            except Exception as e:
                s.rollback()
                print(f"❌ Error pushing NOTAM {src.notam_number} (hash={src.raw_hash}): {e}")

        print(f"✅ Successfully pushed {pushed} NOTAM(s) to Supabase.")

# ---------- CLI ----------

if __name__ == "__main__":
    overwrite = os.getenv("OVERWRITE_SUPABASE", "").strip().lower() in {"1", "true", "yes"}
    push_to_supabase(overwrite=overwrite)
