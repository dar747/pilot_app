# notam/db.py
from __future__ import annotations

import enum
import os
from contextlib import contextmanager
# Remove all the enum class definitions and replace with:
from notam.core.enums import (
    NotamCategoryEnum, SeverityLevelEnum, TimeClassificationEnum,
    TimeOfDayApplicabilityEnum, FlightRuleApplicabilityEnum,
    AircraftSizeEnum, AircraftPropulsionEnum, FlightPhaseEnum,
    PrimaryCategoryEnum
)
from sqlalchemy import (
    create_engine, Column, String, Integer, Float, Boolean, Text, DateTime,
    ForeignKey, Table, JSON, Index, UniqueConstraint, Enum, CheckConstraint,
    SmallInteger, ForeignKeyConstraint
)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy.sql import func
from sqlalchemy import event
from notam.timeutils import parse_iso_to_utc as _parse_iso_to_utc
from dotenv import load_dotenv


load_dotenv()  # does nothing if no .env present
# ---------------------------------------------------------------------------
# Engine & Session
# ---------------------------------------------------------------------------


def get_database_url():
    """Get database URL based on environment and available configs"""
    env = os.getenv("ENVIRONMENT", "development")

    if env == "production":
        # Production: Use production Supabase
        db_url = os.getenv("SUPABASE_DB_URL")
        if not db_url:
            raise RuntimeError("SUPABASE_DB_URL required for production")
        print(f"🎯 Production mode: Using production Supabase")

    elif env == "development":
        # Development: Prefer dev Supabase, fallback to local
        db_url = os.getenv("SUPABASE_DB_DEV_URL") or os.getenv("LOCAL_DB_URL")
        if not db_url:
            raise RuntimeError("SUPABASE_DB_DEV_URL or LOCAL_DB_URL required for development")

        if "supabase.co" in (db_url or ""):
            host = db_url.split('@')[1].split('/')[0] if '@' in db_url else 'supabase'
            print(f"🔌 Development mode: Using dev Supabase ({host})")
        else:
            print(f"🔌 Development mode: Using local database")

    elif env == "staging":
        # Staging: Could use dev Supabase or separate staging
        db_url = os.getenv("SUPABASE_DB_STAGING_URL") or os.getenv("SUPABASE_DB_DEV_URL")
        if not db_url:
            raise RuntimeError("SUPABASE_DB_STAGING_URL or SUPABASE_DB_DEV_URL required for staging")
        print(f"🔌 Staging mode: Using staging/dev Supabase")

    else:
        raise RuntimeError(f"Unknown environment: {env}")

    return db_url


# Get the database URL based on environment
DATABASE_URL = get_database_url()


engine = create_engine(
    DATABASE_URL,
    future=True,
    pool_pre_ping=True,
)
if DATABASE_URL.startswith("postgresql"):
    @event.listens_for(engine, "connect")
    def set_sql_timezone(dbapi_connection, _):
        with dbapi_connection.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")

SessionLocal = sessionmaker(bind=engine, future=True)
Base = declarative_base()


@contextmanager
def get_session():
    """Small helper so you can do: with get_session() as s: ..."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

# ---------------------------------------------------------------------------
# Association (pure many-to-many) Tables
# ---------------------------------------------------------------------------

notam_airports = Table(
    "notam_airports",
    Base.metadata,
    Column("notam_id", Integer, ForeignKey("notams.id", ondelete="CASCADE"), primary_key=True),
    Column("airport_code", String(4), ForeignKey("airports.icao_code", ondelete="CASCADE"), primary_key=True),
    Index("ix_notam_airports_airport_first", "airport_code", "notam_id"),
)

notam_operational_tags = Table(
    "notam_operational_tags",
    Base.metadata,
    Column("notam_id", Integer, ForeignKey("notams.id", ondelete="CASCADE"), nullable=False),
    Column("tag_id", Integer, ForeignKey("operational_tags.id", ondelete="CASCADE"), nullable=False),
    UniqueConstraint("notam_id", "tag_id", name="uq_notam_operational_tags"),
    Index("ix_notam_operational_tags_tag_first", "tag_id", "notam_id"),
)

notam_aircraft_sizes = Table(
    "notam_aircraft_sizes",
    Base.metadata,
    Column("notam_id", Integer, ForeignKey("notams.id", ondelete="CASCADE"), primary_key=True),
    Column("size", Enum(AircraftSizeEnum, native_enum=False), primary_key=True),
    Index("ix_aircraft_sizes_size_first", "size", "notam_id"),
)
# Map a class to the existing association table (no schema duplication)
class NotamAircraftSizeLink(Base):
    __table__ = notam_aircraft_sizes  # reuse the Table you already defined

    # optional relationship back to NotamRecord for convenient access
    notam = relationship("NotamRecord", back_populates="aircraft_size_links", viewonly=True)


notam_aircraft_propulsions = Table(
    "notam_aircraft_propulsions",
    Base.metadata,
    Column("notam_id", Integer, ForeignKey("notams.id", ondelete="CASCADE"), primary_key=True),
    Column("propulsion", Enum(AircraftPropulsionEnum, native_enum=False), primary_key=True),
    Index("ix_aircraft_propulsions_prop_first", "propulsion", "notam_id"),
)

# Map a class to the existing propulsion association table (no schema duplication)
class NotamAircraftPropulsionLink(Base):
    __table__ = notam_aircraft_propulsions  # reuse the Table you already defined

    # optional relationship back to NotamRecord for convenient access
    notam = relationship("NotamRecord", back_populates="aircraft_propulsion_links", viewonly=True)


# ---------------------------------------------------------------------------
# Child (normalized) Tables
# ---------------------------------------------------------------------------

class NotamFlightPhase(Base):
    __tablename__ = "notam_flight_phases"

    notam_id = Column(Integer, ForeignKey("notams.id", ondelete="CASCADE"), primary_key=True)
    phase = Column(Enum(FlightPhaseEnum, native_enum=False), primary_key=True)

    __table_args__ = (
        Index("ix_phase_first", "phase", "notam_id"),
    )

    notam = relationship("NotamRecord", back_populates="flight_phase_links", passive_deletes=True)


class NotamWingspanRestriction(Base):
    __tablename__ = "notam_wingspan_restrictions"

    notam_id = Column(Integer, ForeignKey("notams.id", ondelete="CASCADE"), primary_key=True)
    min_m = Column(Float)
    min_inclusive = Column(Boolean, default=True, nullable=False)
    max_m = Column(Float)
    max_inclusive = Column(Boolean, default=True, nullable=False)

    __table_args__ = (
        CheckConstraint("(min_m IS NULL OR min_m >= 0)", name="chk_wspan_min_nonneg"),
        CheckConstraint("(max_m IS NULL OR max_m >= 0)", name="chk_wspan_max_nonneg"),
        CheckConstraint(
            "(min_m IS NULL OR max_m IS NULL OR min_m <= max_m)",
            name="chk_wspan_min_le_max"
        ),
        Index("ix_wspan_min_max", "min_m", "max_m"),
    )

    notam = relationship(
        "NotamRecord",
        back_populates="wingspan_restriction",
        passive_deletes=True,
        uselist=False
    )


class NotamTaxiway(Base):
    __tablename__ = "notam_taxiways"

    notam_id = Column(Integer, ForeignKey("notams.id", ondelete="CASCADE"), primary_key=True)
    airport_code = Column(String(4), ForeignKey("airports.icao_code", ondelete="CASCADE"), primary_key=True)
    taxiway_id = Column(String(128), primary_key=True)

    __table_args__ = (
        # If you're on Postgres and want a format check, you can add:
        # CheckConstraint("taxiway_id ~ '^[A-Z]{1,3}[0-9]{0,2}$'", name="chk_twy_format"),
        Index("ix_twy_airport_id", "airport_code", "taxiway_id", "notam_id"),
        Index("ix_twy_notam_first", "notam_id", "airport_code", "taxiway_id"),
    )

    notam = relationship("NotamRecord", back_populates="taxiways", passive_deletes=True)


class NotamProcedure(Base):
    __tablename__ = "notam_procedures"

    notam_id = Column(Integer, ForeignKey("notams.id", ondelete="CASCADE"), primary_key=True)
    airport_code = Column(String(4), ForeignKey("airports.icao_code", ondelete="CASCADE"), primary_key=True)
    procedure_name = Column(String(200), primary_key=True)

    __table_args__ = (
        Index("ix_proc_airport_name", "airport_code", "procedure_name", "notam_id"),
        Index("ix_proc_notam_first", "notam_id", "airport_code", "procedure_name"),
    )

    notam = relationship("NotamRecord", back_populates="procedures", passive_deletes=True)


class NotamObstacle(Base):
    __tablename__ = "notam_obstacles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    notam_id = Column(Integer, ForeignKey('notams.id', ondelete="CASCADE"), index=True, nullable=False)
    type = Column(String(128), nullable=False)
    height_agl_ft = Column(Integer, nullable=False)
    height_amsl_ft = Column(Integer)
    latitude = Column(Float)
    longitude = Column(Float)
    lighting = Column(String(32), nullable=False)

class NotamRunway(Base):
    __tablename__ = "notam_runways"

    id = Column(Integer, primary_key=True, autoincrement=True)
    notam_id = Column(Integer, ForeignKey("notams.id", ondelete="CASCADE"), nullable=False, index=True)
    airport_code = Column(String(4), ForeignKey("airports.icao_code", ondelete="CASCADE"), nullable=False)
    runway_number = Column(SmallInteger, nullable=False)     # 1..36
    runway_side = Column(String(1), nullable=True)           # L/C/R or NULL

    __table_args__ = (
        CheckConstraint("runway_number BETWEEN 1 AND 36", name="chk_runway_number"),
        CheckConstraint("runway_side IN ('L','C','R') OR runway_side IS NULL", name="chk_runway_side"),
        UniqueConstraint("notam_id", "airport_code", "runway_number", "runway_side", name="uq_notam_runway_unique"),
        Index("ix_nr_airport_runway", "airport_code", "runway_number", "runway_side", "notam_id"),
    )

    notam = relationship("NotamRecord", back_populates="runways", passive_deletes=True)


class NotamRunwayCondition(Base):
    __tablename__ = "notam_runway_conditions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    notam_id = Column(Integer, ForeignKey('notams.id', ondelete="CASCADE"), nullable=False)
    airport_code = Column(String(4), nullable=False)
    runway_number = Column(SmallInteger, nullable=False)
    runway_side = Column(String(1), nullable=True)
    friction_value = Column(Integer)

    __table_args__ = (
        ForeignKeyConstraint(
            ['notam_id', 'airport_code', 'runway_number', 'runway_side'],
            ['notam_runways.notam_id', 'notam_runways.airport_code', 'notam_runways.runway_number', 'notam_runways.runway_side'],
            ondelete="CASCADE"
        ),
        CheckConstraint("runway_side IN ('L','C','R') OR runway_side IS NULL", name="chk_nrc_runway_side"),
        Index('ix_nrc_airport_runway', 'airport_code', 'runway_number', 'runway_side'),
        Index('ix_nrc_airport_friction', 'airport_code', 'friction_value', 'notam_id'),
    )

    notam = relationship("NotamRecord", back_populates="runway_conditions", passive_deletes=True)


# ---------------------------------------------------------------------------
# Main NOTAM + Lookups
# ---------------------------------------------------------------------------

class NotamRecord(Base):
    __tablename__ = "notams"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Basic
    notam_number = Column(String(50), nullable=False, index=True)
    issue_time = Column(DateTime(timezone=True), nullable=False)

    notam_category = Column(Enum(NotamCategoryEnum, native_enum=False), nullable=False, index=True)
    severity_level = Column(Enum(SeverityLevelEnum, native_enum=False), nullable=False, index=True)

    # Temporal
    start_time = Column(DateTime(timezone=True), nullable=False, index=True)  # CHANGED
    end_time = Column(DateTime(timezone=True), index=True)
    operational_instance = Column(JSON)
    is_active = Column(Boolean, default=True, nullable=False, index=True)

    # Applicability
    time_of_day_applicability = Column(Enum(TimeOfDayApplicabilityEnum, native_enum=False))
    flight_rule_applicability = Column(Enum(FlightRuleApplicabilityEnum, native_enum=False))

    # Single primary category
    primary_category = Column(Enum(PrimaryCategoryEnum, native_enum=False), nullable=False, index=True)


    # Location / Area
    affected_area = Column(JSON)                      # keep JSON for geometry
    affected_airports_snapshot = Column(JSON)         # quick snapshot list

    # Content
    notam_summary = Column(Text, nullable=False)
    one_line_description = Column(Text, nullable=True)
    icao_message = Column(Text)

    # Administrative
    replacing_notam = Column(String(50), index=True)
    raw_hash = Column(String(64), unique=True, index=True)

    # Scoring (server-side base score; client will reweight)
    base_score_vfr = Column(SmallInteger)                 # 0..100
    # short reason
    # Scoring (server-side base score; client will reweight)
    base_score_ifr = Column(SmallInteger)                 # 0..100

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

    # Relationships
    airports = relationship("Airport", secondary=notam_airports, back_populates="notams")
    operational_tags = relationship("OperationalTag", secondary=notam_operational_tags, back_populates="notams", passive_deletes=True)

    wingspan_restriction = relationship("NotamWingspanRestriction", uselist=False, back_populates="notam", cascade="all, delete-orphan")
    taxiways = relationship("NotamTaxiway", cascade="all, delete-orphan")
    procedures = relationship("NotamProcedure", cascade="all, delete-orphan")
    obstacles = relationship("NotamObstacle", cascade="all, delete-orphan")
    runway_conditions = relationship("NotamRunwayCondition", back_populates="notam", cascade="all, delete-orphan", passive_deletes=True, lazy="selectin")
    flight_phase_links = relationship("NotamFlightPhase", cascade="all, delete-orphan", back_populates="notam", lazy="selectin")
    runways = relationship("NotamRunway", back_populates="notam", cascade="all, delete-orphan", passive_deletes=True, lazy="selectin")
    aircraft_size_links = relationship(
        "NotamAircraftSizeLink",
        cascade="all, delete-orphan",
        primaryjoin="NotamRecord.id==NotamAircraftSizeLink.notam_id",
        lazy="selectin",
        viewonly=True,
    )

    @property
    def aircraft_sizes(self):
        return [link.size for link in self.aircraft_size_links]

    # --- propulsions (mirror of sizes) ---
    aircraft_propulsion_links = relationship(
        "NotamAircraftPropulsionLink",
        cascade="all, delete-orphan",
        primaryjoin="NotamRecord.id==NotamAircraftPropulsionLink.notam_id",
        lazy="selectin",
        viewonly=True,
    )

    @property
    def aircraft_propulsions(self):
        return [link.propulsion for link in self.aircraft_propulsion_links]


    __table_args__ = (
        Index('idx_notam_times', 'start_time', 'end_time'),
        UniqueConstraint('notam_number', 'issue_time', name='uq_notam_number_issue'),
    )


# in notam/db.py

class Airport(Base):
    __tablename__ = "airports"

    # Use ICAO as the natural PK — simpler with your current schema
    icao_code = Column(String(4), primary_key=True)

    # Your requested fields (all nullable; fill gradually)
    iata_code = Column(String(3))
    faa_id = Column(String(10))
    name = Column(String(200))
    country = Column(String(100))

    # keep your exact column names
    lat = Column(Float)
    lon = Column(Float)
    elev = Column(Integer)

    freqs = Column(JSON)                    # optional raw JSON from feed
    timezone = Column(String(64))           # e.g., "Asia/Hong_Kong"
    utc_offset_normal = Column(Float)       # hours
    utc_offset_dst = Column(Float)          # hours
    changetodst = Column(DateTime(timezone=True))
    changefromdst = Column(DateTime(timezone=True))

    # if your API gives "2.5E"/"1.3W" strings, store as String;
    # if you prefer numeric, switch to Float and parse to +E/-W
    magnetic_declination = Column(String(16))

    # relationships (unchanged)
    notams = relationship("NotamRecord", secondary=notam_airports, back_populates="airports")

    __table_args__ = (
        Index('idx_airport_location', 'lat', 'lon'),
        Index('idx_airport_country', 'country'),
        Index('idx_airport_iata', 'iata_code'),
    )



class OperationalTag(Base):
    __tablename__ = "operational_tags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_name = Column(String(100), unique=True, nullable=False)

    notams = relationship("NotamRecord", secondary=notam_operational_tags, back_populates="operational_tags", passive_deletes=True)


class NotamHistory(Base):
    __tablename__ = "notam_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    notam_id = Column(Integer, ForeignKey('notams.id', ondelete="CASCADE"), nullable=False)
    action = Column(String(20))  # CREATED, UPDATED, CANCELLED, REPLACED
    changed_fields = Column(JSON)
    timestamp = Column(DateTime(timezone=True), default=func.now())

    __table_args__ = (
        Index('idx_history_notam_time', 'notam_id', 'timestamp'),
    )


class FailedNotam(Base):
    __tablename__ = "failed_notams"

    id = Column(Integer, primary_key=True, autoincrement=True)
    notam_number = Column(String(50), nullable=False, index=True)
    icao_message = Column(Text, nullable=False)
    airport = Column(String(4), nullable=False, index=True)
    issue_time = Column(String(100), nullable=True)
    raw_hash = Column(String(64), nullable=False, index=True)
    failure_reason = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0, nullable=False)
    last_retry_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=func.now(), nullable=False)

    __table_args__ = (
        Index('idx_failed_notams_retry', 'retry_count', 'last_retry_at'),
    )

class PasswordResetCode(Base):
    __tablename__ = "password_reset_codes"

    email = Column(String(255), primary_key=True)
    code = Column(String(6), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), default=func.now(), nullable=False)

    __table_args__ = (
        Index('idx_password_reset_expires', 'expires_at'),
    )


def _ensure_bounds_from_instances(target: "NotamRecord") -> None:
    """
    If operational_instance.operational_instances exists, set:
      start_time = min(start_iso), end_time = max(end_iso)
    (Both stored as aware UTC datetimes.)
    """
    payload = target.operational_instance or {}
    slices = payload.get("operational_instances") or []
    starts, ends = [], []
    for sl in slices:
        try:
            s = _parse_iso_to_utc(sl.get("start_iso"))
            e = _parse_iso_to_utc(sl.get("end_iso"))
            if s and e:
                starts.append(s); ends.append(e)
        except Exception:
            continue
    if starts and ends:
        target.start_time = min(starts)
        target.end_time = max(ends)

    # If someone set issue_time as a string earlier, normalize it too.
    if isinstance(target.issue_time, str):
        target.issue_time = _parse_iso_to_utc(target.issue_time)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def init_db() -> None:
    """Create all tables that don't exist yet."""
    Base.metadata.create_all(bind=engine)


__all__ = [
    # session
    "engine", "SessionLocal", "Base", "get_session", "init_db",
    # enums
    "NotamCategoryEnum", "SeverityLevelEnum", "TimeClassificationEnum",
    "TimeOfDayApplicabilityEnum", "FlightRuleApplicabilityEnum",
    "AircraftSizeEnum", "AircraftPropulsionEnum", "FlightPhaseEnum", "PrimaryCategoryEnum",
    # association tables
    "notam_airports", "notam_operational_tags", "notam_aircraft_sizes", "notam_aircraft_propulsions",
    # link classes (NEW)
    "NotamAircraftSizeLink", "NotamAircraftPropulsionLink",
    # models
    "NotamRecord", "Airport", "OperationalTag", "NotamHistory",
    "NotamWingspanRestriction", "NotamTaxiway", "NotamProcedure", "NotamObstacle",
    "NotamRunway", "NotamRunwayCondition", "NotamFlightPhase","PasswordResetCode",

]
