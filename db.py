import os
from sqlalchemy import create_engine, Column, String, Integer, Text, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_PATH = os.path.join(BASE_DIR, "notams.db")
DATABASE_URL = f"sqlite:///{DATABASE_PATH}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)

class NotamRecord(Base):
    __tablename__ = "notams"
    airport = Column(String, nullable=False)          # Composite PK (part 1)
    notam_number = Column(String, nullable=False)     # Composite PK (part 2)
    issue_time = Column(String)
    notam_info_type = Column(String)
    notam_category = Column(String)
    start_time = Column(String)
    end_time = Column(String)
    seriousness = Column(Integer)
    applied_scenario = Column(String)
    applied_aircraft_type = Column(String)
    operational_tag = Column(Text)
    affected_runway = Column(Text)
    notam_summary = Column(Text)
    icao_message = Column(Text)
    replacing_notam = Column(Text)
    raw_hash = Column(String, unique=True, index=True)   # Optional for content dedupe

    __table_args__ = (
        PrimaryKeyConstraint('airport', 'notam_number'),
    )

def init_db():
    Base.metadata.create_all(bind=engine)
