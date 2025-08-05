import os
from sqlalchemy import create_engine, Column, String, Integer, Text, PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("LOCAL_DB_URL")  # Read from .env file

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
# Base = declarative_base()
#
# class NotamRecord(Base):
#     __tablename__ = "notams"
#     airport = Column(String, nullable=False)
#     notam_number = Column(String, nullable=False)
#     issue_time = Column(String)
#     notam_info_type = Column(String)
#     notam_category = Column(String)
#     start_time = Column(String)
#     end_time = Column(String)
#     seriousness = Column(Integer)
#     applied_scenario = Column(String)
#     applied_aircraft_type = Column(String)
#     operational_tag = Column(Text)
#     affected_runway = Column(Text)
#     notam_summary = Column(Text)
#     icao_message = Column(Text)
#     replacing_notam = Column(Text)
#     raw_hash = Column(String, unique=True, index=True)
#
#
#
#     __table_args__ = (
#         PrimaryKeyConstraint('airport', 'notam_number'),
#     )
#
def init_db():
    Base.metadata.create_all(bind=engine)


from sqlalchemy import (
    Column, String, Integer, Float, Boolean, Text, DateTime,
    ForeignKey, Table, JSON, Index, UniqueConstraint, Enum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime
import enum

Base = declarative_base()


# Enums for database
class SeverityLevelEnum(enum.Enum):
    CRITICAL = "CRITICAL"
    OPERATIONAL = "OPERATIONAL"
    ADVISORY = "ADVISORY"


class UrgencyIndicatorEnum(enum.Enum):
    IMMEDIATE = "IMMEDIATE"
    URGENT = "URGENT"
    ROUTINE = "ROUTINE"
    PLANNED = "PLANNED"


class TimeClassificationEnum(enum.Enum):
    PERMANENT = "PERMANENT"
    LONG_TERM = "LONG_TERM"
    MEDIUM_TERM = "MEDIUM_TERM"
    SHORT_TERM = "SHORT_TERM"
    DAILY = "DAILY"
    EVENT_SPECIFIC = "EVENT_SPECIFIC"


# Association tables for many-to-many relationships
notam_airports = Table('notam_airports', Base.metadata,
                       Column('notam_id', Integer, ForeignKey('notams.id'), primary_key=True),
                       Column('airport_code', String(4), ForeignKey('airports.icao_code'), primary_key=True),
                       Index('idx_notam_airports', 'notam_id', 'airport_code')
                       )

notam_runways = Table(
    'notam_runways',
    Base.metadata,
    Column('notam_id', Integer, ForeignKey('notams.id'), primary_key=True),
    Column('airport_code', String(4), ForeignKey('airports.icao_code'), primary_key=True),
    Column('runway_id', String(7), primary_key=True),  # e.g., "09L/27R"
    Index('idx_notam_runways', 'notam_id', 'airport_code', 'runway_id')
)


notam_operational_tags = Table('notam_operational_tags', Base.metadata,
                               Column('notam_id', Integer, ForeignKey('notams.id'), primary_key=True),
                                     Column('airport_code', String(4), ForeignKey('airports.icao_code'), primary_key=True),
                               Column('tag_id', Integer, ForeignKey('operational_tags.id'), primary_key=True),

                               Index('idx_notam_tags', 'notam_id', 'tag_id')
                               )

notam_filter_tags = Table('notam_filter_tags', Base.metadata,
                          Column('notam_id', Integer, ForeignKey('notams.id'), primary_key=True),
                          Column('airport_code', String(4), ForeignKey('airports.icao_code'), primary_key=True),
                          Column('tag_id', Integer, ForeignKey('filter_tags.id'), primary_key=True),
                          Index('idx_notam_filter_tags', 'notam_id', 'tag_id')
                          )



# Main NOTAM table
class NotamRecord(Base):
    __tablename__ = "notams"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Basic Information (maintaining compatibility)
    notam_number = Column(String(50), nullable=False, index=True)
    issue_time = Column(DateTime, nullable=False)
    notam_info_type = Column(String(10))  # Q-code
    notam_category = Column(String(20))  # FIR or Airport

    # Enhanced Severity Classification
    seriousness = Column(Integer)  # Legacy 1-3
    severity_level = Column(Enum(SeverityLevelEnum), nullable=False, index=True)
    urgency_indicator = Column(Enum(UrgencyIndicatorEnum), nullable=False)

    # Temporal Information
    start_time = Column(DateTime, nullable=False, index=True)
    end_time = Column(DateTime, index=True)  # Nullable for permanent
    time_classification = Column(Enum(TimeClassificationEnum), nullable=False)
    schedule = Column(Text)  # Daily/weekly schedule if applicable

    # Applicability
    applied_scenario = Column(String(20))  # Legacy field
    applied_aircraft_type = Column(String(20))  # Legacy field
    aircraft_categories = Column(JSON)  # List stored as JSON
    flight_phases = Column(JSON)  # List stored as JSON

    # Categorization
    primary_category = Column(String(100), index=True)
    secondary_categories = Column(JSON)  # List stored as JSON

    # Location Information (normalized)
    affected_fir = Column(String(10))
    affected_coordinate = Column(String(100))  # Legacy field
    affected_area = Column(JSON)  # Complex structure as JSON

    # Content
    notam_summary = Column(Text, nullable=False)
    icao_message = Column(Text)  # Original NOTAM text
    raw_text = Column(Text)

    # Infrastructure Impact (JSON for complex structures)
    extracted_elements = Column(JSON)  # All extracted technical elements

    # Operational Analysis (JSON for complex structures)
    operational_impact = Column(JSON)
    safety_assessment = Column(JSON)

    # Administrative
    replacing_notam = Column(String(50), index=True)
    replaced_by = Column(String(50))
    related_notams = Column(JSON)  # List of related NOTAM numbers

    # Multi-category Support
    multi_category_rationale = Column(Text)

    # App-specific Fields
    requires_acknowledgment = Column(Boolean, default=False, index=True)
    display_priority = Column(Integer, nullable=False, index=True)

    # Validation and Quality
    confidence_score = Column(Float)
    validation_warnings = Column(JSON)  # List stored as JSON

    # Tracking
    raw_hash = Column(String(64), unique=True, index=True)  # For deduplication
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    last_validated = Column(DateTime)

    # Status
    is_active = Column(Boolean, default=True, index=True)
    is_cancelled = Column(Boolean, default=False)
    cancelled_by = Column(String(50))  # NOTAM number that cancelled this

    # Relationships
    airports = relationship("Airport", secondary=notam_airports, back_populates="notams")
    operational_tags = relationship("OperationalTag", secondary=notam_operational_tags, back_populates="notams")
    filter_tags = relationship("FilterTag", secondary=notam_filter_tags, back_populates="notams")
    acknowledgments = relationship("NotamAcknowledgment", back_populates="notam")

    # Indexes
    __table_args__ = (
        Index('idx_notam_times', 'start_time', 'end_time'),
        Index('idx_notam_active', 'is_active', 'start_time', 'end_time'),
        Index('idx_notam_severity_priority', 'severity_level', 'display_priority'),
        UniqueConstraint('notam_number', 'issue_time', name='uq_notam_number_issue'),
    )


# Airport reference table
class Airport(Base):
    __tablename__ = "airports"

    icao_code = Column(String(4), primary_key=True)
    iata_code = Column(String(3))
    name = Column(String(200))
    city = Column(String(100))
    country = Column(String(100))
    latitude = Column(Float)
    longitude = Column(Float)
    elevation_ft = Column(Integer)

    # Relationships
    notams = relationship("NotamRecord", secondary=notam_airports, back_populates="airports")

    # Index
    __table_args__ = (
        Index('idx_airport_location', 'latitude', 'longitude'),
    )


# Operational Tags lookup table
class OperationalTag(Base):
    __tablename__ = "operational_tags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_name = Column(String(100), unique=True, nullable=False)
    category = Column(String(50))  # Tag category for grouping
    description = Column(Text)
    is_critical = Column(Boolean, default=False)

    # Relationships
    notams = relationship("NotamRecord", secondary=notam_operational_tags, back_populates="operational_tags")


# Filter Tags lookup table
class FilterTag(Base):
    __tablename__ = "filter_tags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_name = Column(String(100), unique=True, nullable=False)
    tag_category = Column(String(50))  # For UI grouping
    display_order = Column(Integer)
    is_default = Column(Boolean, default=False)  # Default tags for new users

    # Relationships
    notams = relationship("NotamRecord", secondary=notam_filter_tags, back_populates="filter_tags")


# User acknowledgments tracking
class NotamAcknowledgment(Base):
    __tablename__ = "notam_acknowledgments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    notam_id = Column(Integer, ForeignKey('notams.id'), nullable=False)
    user_id = Column(String(100), nullable=False)  # Your user ID system
    acknowledged_at = Column(DateTime, default=func.now())
    flight_number = Column(String(20))  # Optional flight association

    # Relationships
    notam = relationship("NotamRecord", back_populates="acknowledgments")

    # Index
    __table_args__ = (
        Index('idx_ack_user_notam', 'user_id', 'notam_id'),
        UniqueConstraint('notam_id', 'user_id', 'flight_number', name='uq_user_notam_flight'),
    )


# User saved filters
class SavedFilter(Base):
    __tablename__ = "saved_filters"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(100), nullable=False, index=True)
    filter_name = Column(String(100), nullable=False)
    filter_config = Column(JSON)  # Stores complete filter configuration
    is_default = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint('user_id', 'filter_name', name='uq_user_filter_name'),
    )


# NOTAM history tracking
class NotamHistory(Base):
    __tablename__ = "notam_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    notam_id = Column(Integer, ForeignKey('notams.id'), nullable=False)
    action = Column(String(20))  # CREATED, UPDATED, CANCELLED, REPLACED
    changed_fields = Column(JSON)  # What changed
    timestamp = Column(DateTime, default=func.now())

    # Index
    __table_args__ = (
        Index('idx_history_notam_time', 'notam_id', 'timestamp'),
    )

#
# # Helper functions for database operations
# def create_or_update_notam(session, notam_data, extracted_tags):
#     """
#     Create or update a NOTAM record with all relationships
#     """
#     # Check if NOTAM exists
#     existing = session.query(NotamRecord).filter_by(
#         notam_number=notam_data['notam_number'],
#         issue_time=notam_data['issue_time']
#     ).first()
#
#     if existing:
#         # Update existing record
#         for key, value in notam_data.items():
#             if hasattr(existing, key):
#                 setattr(existing, key, value)
#         notam = existing
#     else:
#         # Create new record
#         notam = NotamRecord(**notam_data)
#         session.add(notam)
#
#     # Handle airport relationships
#     if 'affected_airports' in extracted_tags:
#         notam.airports = []
#         for icao in extracted_tags['affected_airports']:
#             airport = session.query(Airport).filter_by(icao_code=icao).first()
#             if airport:
#                 notam.airports.append(airport)
#
#     # Handle operational tags
#     if 'operational_tags' in extracted_tags:
#         notam.operational_tags = []
#         for tag_name in extracted_tags['operational_tags']:
#             tag = session.query(OperationalTag).filter_by(tag_name=tag_name).first()
#             if not tag:
#                 tag = OperationalTag(tag_name=tag_name)
#                 session.add(tag)
#             notam.operational_tags.append(tag)
#
#     # Handle filter tags
#     if 'filter_tags' in extracted_tags:
#         notam.filter_tags = []
#         for tag_name in extracted_tags['filter_tags']:
#             tag = session.query(FilterTag).filter_by(tag_name=tag_name).first()
#             if not tag:
#                 tag = FilterTag(tag_name=tag_name)
#                 session.add(tag)
#             notam.filter_tags.append(tag)
#
#     session.commit()
#     return notam
