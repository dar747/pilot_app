# notam/core/repository.py
from sqlalchemy.orm import Session
from notam.db import SessionLocal, NotamRecord
from typing import List, Dict, Optional, Set
import hashlib


class NotamRepository:
    def __init__(self, session: Optional[Session] = None):
        self._session = session
        self._owns_session = session is None

    @property
    def session(self):
        if self._session is None:
            self._session = SessionLocal()
        return self._session

    def get_existing_hashes(self) -> Set[str]:
        """Get all existing NOTAM hashes from database"""
        # Move logic from persistence.py

    def save_notam(self, result, raw_text: str, notam_number: str, raw_hash: str, airport_code: str) -> Optional[int]:
        """Save single NOTAM to database"""
        # Consolidate save_to_db logic

    def save_batch(self, batch_results: List[Dict]) -> None:
        """Save batch of analyzed NOTAMs"""
        from notam.services.persistence import save_results_batch
        save_results_batch(batch_results)
        # Move from persistence.py

    def delete_by_ids(self, ids: List[int]) -> int:
        """Delete NOTAMs by database IDs"""

    def get_hash(self, notam_number: str, icao_message: str) -> str:
        """Generate hash for NOTAM deduplication"""
        combined = f"{notam_number.strip()}|{icao_message.strip()}"
        return hashlib.sha256(combined.encode("utf-8")).hexdigest()