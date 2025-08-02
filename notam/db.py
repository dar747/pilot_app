import os
from sqlalchemy import create_engine, Column, String, Integer, Text, PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("LOCAL_DB_URL")  # Read from .env file

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class NotamRecord(Base):
    __tablename__ = "notams"
    airport = Column(String, nullable=False)
    notam_number = Column(String, nullable=False)
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
    raw_hash = Column(String, unique=True, index=True)

    __table_args__ = (
        PrimaryKeyConstraint('airport', 'notam_number'),
    )

def init_db():
    Base.metadata.create_all(bind=engine)
