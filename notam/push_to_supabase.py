# notam/push_to_supabase.py
import os
import sys
from contextlib import contextmanager

from sqlalchemy import create_engine, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, joinedload
from dotenv import load_dotenv

from notam.db import (
    Base,
    # Core models
    NotamRecord, Airport, OperationalTag, NotamHistory,
    # Link tables (ORM-mapped classes)
    NotamAircraftSizeLink, NotamAircraftPropulsionLink, NotamFlightPhase,
    # Children
    NotamWingspanRestriction, NotamTaxiway, NotamProcedure, NotamObstacle,
    NotamRunway, NotamRunwayCondition,
    # Pure m2m table (for manual inserts if needed)
    notam_operational_tags,
)

# ---------- env / engines ----------

load_dotenv()

LOCAL_DB_URL = os.getenv("LOCAL_DB_URL")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_DEV_URL")

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

# For local reads: keep attributes alive after commit
LocalSession = sessionmaker(bind=local_engine, future=True, expire_on_commit=False)
# For remote writes: default behavior is fine
SupabaseSession = sessionmaker(bind=supabase_engine, future=True)


@contextmanager
def local_session():
    s = LocalSession()
    try:
        yield s
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
    """
    Pull NOTAMs with all relationships eagerly loaded so we can safely
    copy them after the session closes.
    """
    with local_session() as s:
        q = (
            s.query(NotamRecord)
            .options(
                joinedload(NotamRecord.airports),
                joinedload(NotamRecord.operational_tags),

                joinedload(NotamRecord.aircraft_size_links),
                joinedload(NotamRecord.aircraft_propulsion_links),

                joinedload(NotamRecord.flight_phase_links),
                joinedload(NotamRecord.wingspan_restriction),
                joinedload(NotamRecord.taxiways),
                joinedload(NotamRecord.procedures),
                joinedload(NotamRecord.obstacles),
                joinedload(NotamRecord.runways),
                joinedload(NotamRecord.runway_conditions),
            )
        )
        records = q.all()

        # Also prefetch histories by notam_id to avoid n+1 later
        by_id = {r.id: r for r in records}
        histories = (
            s.query(NotamHistory)
            .filter(NotamHistory.notam_id.in_(list(by_id.keys())))
            .all()
        )
        # attach a transient attr for convenience (not ORM relationship)
        from collections import defaultdict
        hist_map = defaultdict(list)
        for h in histories:
            hist_map[h.notam_id].append(h)
        for r in records:
            r._local_histories = hist_map.get(r.id, [])

        return records


def get_supabase_hashes():
    with remote_session() as s:
        try:
            return {h for (h,) in s.query(NotamRecord.raw_hash).all() if h}
        except Exception as e:
            print(f"❌ Error reading Supabase hashes: {e}")
            return set()


def ensure_remote_schema():
    # Will create tables if they don't exist (Postgres on Supabase)
    Base.metadata.create_all(bind=supabase_engine)


def clear_supabase():
    with remote_session() as s:
        print("⚠️  Deleting all data from Supabase tables (respecting FKs)…")
        for tbl in reversed(Base.metadata.sorted_tables):
            s.execute(tbl.delete())
        print("🧹 Supabase cleared.")


# ---------- copy helpers for children ----------

def upsert_airport_stub_or_copy(s, existing_airports, local_airport: Airport) -> Airport:
    ap = existing_airports.get(local_airport.icao_code)
    if ap:
        # Optionally refresh some fields to keep closer to local
        updated = False
        for field in (
            "iata_code", "faa_id", "name", "country",
            "lat", "lon", "elev",
            "freqs", "timezone", "utc_offset_normal", "utc_offset_dst",
            "changetodst", "changefromdst", "magnetic_declination",
        ):
            lv = getattr(local_airport, field)
            rv = getattr(ap, field)
            if lv is not None and lv != rv:
                setattr(ap, field, lv)
                updated = True
        if updated:
            s.flush()
        return ap

    # Create new, copying all known fields
    ap = Airport(
        icao_code=local_airport.icao_code,
        iata_code=local_airport.iata_code,
        faa_id=local_airport.faa_id,
        name=local_airport.name or f"{local_airport.icao_code} Airport",
        country=local_airport.country,
        lat=local_airport.lat,
        lon=local_airport.lon,
        elev=local_airport.elev,
        freqs=local_airport.freqs,
        timezone=local_airport.timezone,
        utc_offset_normal=local_airport.utc_offset_normal,
        utc_offset_dst=local_airport.utc_offset_dst,
        changetodst=local_airport.changetodst,
        changefromdst=local_airport.changefromdst,
        magnetic_declination=local_airport.magnetic_declination,
    )
    s.add(ap); s.flush()
    existing_airports[ap.icao_code] = ap
    return ap


def copy_children_and_links(s, src: NotamRecord, new: NotamRecord, existing_airports, existing_op_tags):
    # -- Airports (m2m) --
    for a in src.airports:
        ap = upsert_airport_stub_or_copy(s, existing_airports, a)
        if ap not in new.airports:
            new.airports.append(ap)

    # -- Operational tags (m2m) --
    op_links = set()
    for t in src.operational_tags:
        tag = existing_op_tags.get(t.tag_name)
        if not tag:
            tag = OperationalTag(tag_name=t.tag_name)
            s.add(tag); s.flush()
            existing_op_tags[t.tag_name] = tag
        key = (new.id, tag.id)
        if key not in op_links:
            try:
                # Use explicit insert to avoid accidental duplicates
                s.execute(insert(notam_operational_tags).values(
                    notam_id=new.id, tag_id=tag.id
                ))
                op_links.add(key)
            except IntegrityError:
                s.rollback()  # already exists

    # -- Aircraft sizes (association table via ORM class) --
    for link in src.aircraft_size_links or []:
        new.aircraft_size_links.append(
            NotamAircraftSizeLink(size=link.size)
        )

    # -- Aircraft propulsions (association table via ORM class) --
    for link in src.aircraft_propulsion_links or []:
        new.aircraft_propulsion_links.append(
            NotamAircraftPropulsionLink(propulsion=link.propulsion)
        )

    # -- Flight phases (child table) --
    for fp in src.flight_phase_links or []:
        new.flight_phase_links.append(
            NotamFlightPhase(phase=fp.phase)
        )

    # -- Wingspan restriction (1:1 child) --
    if src.wingspan_restriction:
        w = src.wingspan_restriction
        new.wingspan_restriction = NotamWingspanRestriction(
            min_m=w.min_m,
            min_inclusive=w.min_inclusive,
            max_m=w.max_m,
            max_inclusive=w.max_inclusive,
        )

    # -- Taxiways (child) --
    for t in src.taxiways or []:
        new.taxiways.append(
            NotamTaxiway(
                airport_code=t.airport_code,
                taxiway_id=t.taxiway_id,
            )
        )

    # -- Procedures (child) --
    for p in src.procedures or []:
        new.procedures.append(
            NotamProcedure(
                airport_code=p.airport_code,
                procedure_name=p.procedure_name,
            )
        )

    # -- Obstacles (child, has own PK) --
    for o in src.obstacles or []:
        new.obstacles.append(
            NotamObstacle(
                type=o.type,
                height_agl_ft=o.height_agl_ft,
                height_amsl_ft=o.height_amsl_ft,
                latitude=o.latitude,
                longitude=o.longitude,
                lighting=o.lighting
            )
        )

    # -- Runways (child) --
    # Build an index so conditions can point to the right composite key
    for rwy in src.runways or []:
        new.runways.append(
            NotamRunway(
                airport_code=rwy.airport_code,
                runway_number=rwy.runway_number,
                runway_side=rwy.runway_side,
            )
        )
    s.flush()  # ensure new.runways have PKs & notam_id set

    # -- Runway conditions (child referencing composite FK to runways) --
    for rc in src.runway_conditions or []:
        new.runway_conditions.append(
            NotamRunwayCondition(
                airport_code=rc.airport_code,
                runway_number=rc.runway_number,
                runway_side=rc.runway_side,
                friction_value=rc.friction_value,
            )
        )


def copy_histories(s_remote, local_histories):
    for h in local_histories or []:
        rec = NotamHistory(
            notam_id=None,  # will be set by relationship if we attach; here we just set fk explicitly later
            action=h.action,
            changed_fields=h.changed_fields,
            timestamp=h.timestamp,
        )
        s_remote.add(rec)
        # We'll set notam_id below when we know the new NOTAM id
    s_remote.flush()


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
        # Cache existing small lookup tables
        existing_airports = {a.icao_code: a for a in s.query(Airport).all()}
        existing_op_tags = {t.tag_name: t for t in s.query(OperationalTag).all()}

        pushed = 0

        for src in records_to_push:
            try:
                # Create top-level NOTAM with all new fields reflected
                new = NotamRecord(
                    notam_number=src.notam_number,
                    issue_time=src.issue_time,

                    notam_category=src.notam_category,
                    severity_level=src.severity_level,

                    start_time=src.start_time,
                    end_time=src.end_time,
                    operational_instance=src.operational_instance,

                    time_of_day_applicability=src.time_of_day_applicability,
                    flight_rule_applicability=src.flight_rule_applicability,

                    primary_category=src.primary_category,

                    affected_area=src.affected_area,
                    affected_airports_snapshot=src.affected_airports_snapshot,

                    notam_summary=src.notam_summary,
                    one_line_description=src.one_line_description,
                    icao_message=src.icao_message,

                    replacing_notam=src.replacing_notam,
                    raw_hash=src.raw_hash,

                    base_score_vfr=src.base_score_vfr,
                    base_score_ifr=src.base_score_ifr,
                )
                s.add(new)
                s.flush()  # get new.id

                # Deep-copy all relationships/children
                copy_children_and_links(
                    s, src=src, new=new,
                    existing_airports=existing_airports,
                    existing_op_tags=existing_op_tags
                )

                # Copy histories (repoint to new NOTAM id)
                for h in getattr(src, "_local_histories", []):
                    s.add(NotamHistory(
                        notam_id=new.id,
                        action=h.action,
                        changed_fields=h.changed_fields,
                        timestamp=h.timestamp,
                    ))

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
    push_to_supabase(overwrite=False)
