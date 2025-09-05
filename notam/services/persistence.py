# notam/services/persistence.py
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Iterable
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
import hashlib
from notam.timeutils import parse_iso_to_utc, to_z
from notam.db import (
    SessionLocal,
    NotamRecord, Airport, OperationalTag, NotamHistory,
    NotamWingspanRestriction, NotamTaxiway, NotamProcedure, NotamObstacle,
    NotamRunway, NotamRunwayCondition, NotamFlightPhase,
    notam_aircraft_propulsions, notam_aircraft_sizes,
)
from notam.scoring import compute_base_score
from notam.core.enums import (
    SeverityLevelEnum, TimeOfDayApplicabilityEnum,
    FlightRuleApplicabilityEnum, AircraftSizeEnum, AircraftPropulsionEnum,
    PrimaryCategoryEnum, NotamCategoryEnum, FlightPhaseEnum
)

log = logging.getLogger(__name__)

def get_hash(notam_number, icao_message):
    combined = f"{notam_number.strip()}|{icao_message.strip()}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()

def get_existing_hashes():
    session = SessionLocal()
    try:
        hashes = set(x[0] for x in session.query(NotamRecord.raw_hash).all() if x[0])
        log.info(f"🔎 DB contains {len(hashes)} existing NOTAM hashes.")
        return hashes
    finally:
        session.close()

def get_raw_hashes_for_notam_ids(ids: Iterable[int]) -> set[str]:
    ids = {int(i) for i in ids if i is not None}
    if not ids:
        return set()
    session = SessionLocal()
    try:
        rows = session.query(NotamRecord.raw_hash).filter(NotamRecord.id.in_(ids)).all()
        return {h for (h,) in rows if h}
    finally:
        session.close()


def delete_notams_by_ids(session: Session, ids: Iterable[int]) -> int:
    """Hard-delete NOTAMs and dependent rows for the given DB ids."""
    ids = sorted({int(i) for i in ids if i is not None})
    if not ids:
        return 0

    # delete children first (if you don't have cascade="all, delete-orphan")
    session.query(NotamFlightPhase).filter(NotamFlightPhase.notam_id.in_(ids))\
        .delete(synchronize_session=False)
    session.query(NotamWingspanRestriction).filter(NotamWingspanRestriction.notam_id.in_(ids))\
        .delete(synchronize_session=False)
    session.query(NotamTaxiway).filter(NotamTaxiway.notam_id.in_(ids))\
        .delete(synchronize_session=False)
    session.query(NotamProcedure).filter(NotamProcedure.notam_id.in_(ids))\
        .delete(synchronize_session=False)
    session.query(NotamObstacle).filter(NotamObstacle.notam_id.in_(ids))\
        .delete(synchronize_session=False)
    session.query(NotamRunwayCondition).filter(NotamRunwayCondition.notam_id.in_(ids))\
        .delete(synchronize_session=False)
    session.query(NotamRunway).filter(NotamRunway.notam_id.in_(ids))\
        .delete(synchronize_session=False)
    session.query(NotamHistory).filter(NotamHistory.notam_id.in_(ids))\
        .delete(synchronize_session=False)

    deleted = session.query(NotamRecord).filter(NotamRecord.id.in_(ids))\
        .delete(synchronize_session=False)
    log.info("🔁 Overwrite by id: deleted %d NOTAM(s): %s", deleted, ids)
    return deleted

def truncate_all_notams(session: Session, restart_identity: bool = False) -> None:
    """
    Fast wipe using PostgreSQL TRUNCATE CASCADE.
    Set restart_identity=True if you want PK sequences reset.
    """
    stmt = "TRUNCATE TABLE notams CASCADE"  # __tablename__ is "notams"
    if restart_identity:
        stmt += " RESTART IDENTITY"
    session.execute(text(stmt))
    log.warning("🧨 Overwrite-all: TRUNCATE CASCADE on notams%s.",
                " + RESTART IDENTITY" if restart_identity else "")

def clear_db():
    session = SessionLocal()
    try:
        with session.begin():
            truncate_all_notams(session, restart_identity=False) ##(If you want IDs to reset, change to restart_identity=True.)
    except Exception:
        log.exception("❌ Failed to clear database")
        raise
    finally:
        session.close()

def _none_if_nullish(x):
    return None if (x is None or (isinstance(x, str) and x.strip().upper() in {"", "NULL", "NONE"})) else x

def parse_runway_id(runway_id: str) -> Tuple[Optional[int], Optional[str]]:
    if not runway_id:
        return None, None
    s = runway_id.strip().upper()
    side = s[-1] if s[-1:] in {'L','C','R'} else None
    if side:
        s = s[:-1]
    try:
        num = int(s)
        return (num, side) if 1 <= num <= 36 else (None, side)
    except ValueError:
        return None, side

def save_to_db(
    result,
    raw_text: str,
    notam_number: str,
    raw_hash: str,
    airport_code: str,
    session: Optional[Session] = None,
    autocommit: bool = True,
) -> Optional[int]:
    """
    Upsert a single analyzed NOTAM.

    If `session` is provided and `autocommit=False`, this function will only flush
    (no commit/rollback). Exceptions will be raised to the caller so they can be
    handled inside a SAVEPOINT (session.begin_nested()).
    """
    owns_session = False
    if session is None:
        session = SessionLocal()
        owns_session = True

    try:
        notam = session.query(NotamRecord).filter_by(raw_hash=raw_hash).first()
        is_update = bool(notam)
        if not is_update:
            notam = NotamRecord(raw_hash=raw_hash)

        # Core fields
        notam.notam_number = notam_number
        notam.notam_category = NotamCategoryEnum(result.notam_category.value)
        notam.severity_level = SeverityLevelEnum(result.severity_level.value)
        notam.issue_time = parse_iso_to_utc(result.issue_time)

        # operational instances JSON
        ops_array = []
        if getattr(result, "operational_instances", None):
            for sl in result.operational_instances:
                s = to_z(parse_iso_to_utc(sl.start_iso))
                e = to_z(parse_iso_to_utc(sl.end_iso))
                if s and e:
                    ops_array.append({"start_iso": s, "end_iso": e})
        notam.operational_instance = {"operational_instances": ops_array}

        # start/end bounds
        if ops_array:
            starts = [parse_iso_to_utc(s["start_iso"]) for s in ops_array]
            ends = [parse_iso_to_utc(s["end_iso"]) for s in ops_array]
            notam.start_time = min(starts)
            notam.end_time = max(ends)
        else:
            notam.start_time = parse_iso_to_utc(getattr(result, "start_time", None)) or parse_iso_to_utc(result.issue_time)
            notam.end_time = parse_iso_to_utc(_none_if_nullish(getattr(result, "end_time", None)))

        notam.time_of_day_applicability = TimeOfDayApplicabilityEnum(result.time_of_day_applicability.value)
        notam.flight_rule_applicability = FlightRuleApplicabilityEnum(result.flight_rule_applicability.value)
        notam.primary_category = PrimaryCategoryEnum(result.primary_category.value)

        # content
        notam.affected_area = result.affected_area.model_dump(exclude_none=True) if result.affected_area else None
        notam.affected_airports_snapshot = result.affected_airports or []
        notam.notam_summary = result.notam_summary
        notam.one_line_description = result.one_line_description
        notam.icao_message = raw_text
        notam.replacing_notam = result.replacing_notam or None

        if not is_update:
            session.add(notam)
            session.flush()  # ensure notam.id

        # airports
        def get_or_create_airport(code: str) -> Airport:
            ap = session.query(Airport).filter_by(icao_code=code).first()
            if not ap:
                ap = Airport(icao_code=code, name=f"{code} Airport")
                session.add(ap); session.flush()
            return ap

        primary_ap = get_or_create_airport(airport_code)
        if is_update:
            notam.airports.clear()
        if primary_ap not in notam.airports:
            notam.airports.append(primary_ap)

        for icao in (result.affected_airports or []):
            if icao and icao != airport_code:
                ap = get_or_create_airport(icao)
                if ap not in notam.airports:
                    notam.airports.append(ap)

        # tags
        if is_update:
            notam.operational_tags.clear()
        for tag_name in (result.operational_tag or []):
            tag = session.query(OperationalTag).filter_by(tag_name=tag_name).first()
            if not tag:
                tag = OperationalTag(tag_name=tag_name)
                session.add(tag); session.flush()
            if tag not in notam.operational_tags:
                notam.operational_tags.append(tag)

        # phases
        session.query(NotamFlightPhase).filter_by(notam_id=notam.id).delete()
        for p in (result.flight_phases or []):
            session.add(NotamFlightPhase(notam_id=notam.id, phase=FlightPhaseEnum(p.value)))

        # aircraft applicability
        aa = result.aircraft_applicability
        session.execute(notam_aircraft_sizes.delete().where(notam_aircraft_sizes.c.notam_id == notam.id))
        session.execute(notam_aircraft_propulsions.delete().where(notam_aircraft_propulsions.c.notam_id == notam.id))
        ws_old = session.query(NotamWingspanRestriction).filter_by(notam_id=notam.id).first()
        if ws_old:
            session.delete(ws_old)

        for s in (aa.sizes or []):
            session.execute(
                notam_aircraft_sizes.insert().values(notam_id=notam.id, size=AircraftSizeEnum(s.value))
            )
        for pr in (aa.propulsion or []):
            session.execute(
                notam_aircraft_propulsions.insert().values(notam_id=notam.id, propulsion=AircraftPropulsionEnum(pr.value))
            )

        ws = getattr(aa, "wingspan_restriction", None)
        if ws and any(v is not None for v in (ws.min_m, ws.max_m)):
            session.add(NotamWingspanRestriction(
                notam_id=notam.id,
                min_m=ws.min_m, min_inclusive=ws.min_inclusive,
                max_m=ws.max_m, max_inclusive=ws.max_inclusive
            ))

        # extracted elements
        ee = getattr(result, "extracted_elements", None)

        session.query(NotamTaxiway).filter_by(notam_id=notam.id).delete()
        session.query(NotamProcedure).filter_by(notam_id=notam.id).delete()
        if ee:
            for t in (ee.taxiways or []):
                if t:
                    session.add(NotamTaxiway(
                        notam_id=notam.id, airport_code=primary_ap.icao_code, taxiway_id=str(t).upper()
                    ))
            for pr in (ee.procedures or []):
                if pr:
                    session.add(NotamProcedure(
                        notam_id=notam.id, airport_code=primary_ap.icao_code, procedure_name=str(pr).upper()
                    ))

        session.query(NotamObstacle).filter_by(notam_id=notam.id).delete()
        if ee:
            for o in (ee.obstacles or []):
                session.add(NotamObstacle(
                    notam_id=notam.id,
                    type=o.type,
                    height_agl_ft=o.height_agl_ft,
                    height_amsl_ft=o.height_amsl_ft,
                    latitude=(o.location.latitude if o.location else None),
                    longitude=(o.location.longitude if o.location else None),
                    lighting=o.lighting
                ))

        session.query(NotamRunway).filter_by(notam_id=notam.id, airport_code=primary_ap.icao_code).delete()
        if ee:
            for rwy in (ee.runways or []):
                num, side = parse_runway_id(str(rwy))
                if num is not None:
                    session.add(NotamRunway(
                        notam_id=notam.id,
                        airport_code=primary_ap.icao_code,
                        runway_number=num,
                        runway_side=side
                    ))

        session.query(NotamRunwayCondition).filter_by(notam_id=notam.id).delete()
        if ee:
            for rc in (ee.runway_conditions or []):
                num, side = parse_runway_id(getattr(rc, "runway_id", None))
                if num is not None:
                    session.add(NotamRunwayCondition(
                        notam_id=notam.id,
                        airport_code=primary_ap.icao_code,
                        runway_number=num,
                        runway_side=side,
                        friction_value=rc.friction_value
                    ))

        # scores
        score_ifr, _, _ = compute_base_score(notam, profile="IFR")
        score_vfr, _, _ = compute_base_score(notam, profile="VFR")
        notam.base_score_ifr = score_ifr
        notam.base_score_vfr = score_vfr

        # history
        session.add(NotamHistory(
            notam_id=notam.id,
            action=("UPDATED" if is_update else "CREATED"),
            changed_fields={"updated_at": datetime.now(timezone.utc).isoformat()} if is_update else {}
        ))

        # finalize write
        if autocommit:
            session.commit()
        else:
            session.flush()  # leave transaction open for caller

        log.info("📝 %s %s at %s", "Updated" if is_update else "Saved", notam_number, airport_code)
        return notam.id

    except IntegrityError:
        if autocommit:
            session.rollback()
            log.warning("⚠️ Integrity error on %s (hash=%s)", notam_number, raw_hash)
            return None
        # let caller's SAVEPOINT handle it
        raise
    except Exception:
        if autocommit:
            session.rollback()
            log.exception("❌ DB error on %s (hash=%s)", notam_number, raw_hash)
            return None
        # propagate inside nested transaction
        raise
    finally:
        if owns_session:
            session.close()


def save_results_batch(
    batch_results: List[Dict],
    *,
    overwrite_all: bool = False,
    overwrite_db_ids: Optional[Iterable[int]] = None,
        ):
    """
    Persist a batch in a single outer transaction.
    Each item is wrapped in a SAVEPOINT (begin_nested), so failures don't
    abort the whole batch.
    batch_results items are {'input': item, 'result': pydantic_or_None, 'error': str_or_None}
    """
    session = SessionLocal()
    try:
        # Outer transaction (committed once at the end)
        with session.begin():
            if overwrite_all:
                truncate_all_notams(session,restart_identity=False)
            elif overwrite_db_ids:
                delete_notams_by_ids(session, overwrite_db_ids)


            for r in batch_results:
                item = r["input"]
                res = r["result"]
                if res is None:
                    log.error("Skipping %s due to analysis error: %s", item.get("notam_number"), r["error"])
                    continue

                try:
                    # SAVEPOINT per item
                    with session.begin_nested():
                        save_to_db(
                            result=res,
                            raw_text=item["icao_message"],
                            notam_number=item["notam_number"],
                            raw_hash=item["raw_hash"],
                            airport_code=item.get("airport", "Unknown"),
                            session=session,
                            autocommit=False,  # defer; outer ctx manages commit
                        )
                except IntegrityError:
                    log.warning("⚠️ Skipped %s due to integrity error", item.get("notam_number"))
                except Exception:
                    log.exception("❌ Skipped %s due to DB error", item.get("notam_number"))

        # if we reach here, outer transaction committed successfully
    except Exception:
        log.exception("❌ Batch save failed")
        raise
    finally:
        session.close()
