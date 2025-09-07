# notam/services/retry_failed.py
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict
from sqlalchemy.orm import sessionmaker
from notam.db import SessionLocal, FailedNotam
from notam.services.analyser import analyze_many
from notam.services.persistence import save_results_batch

log = logging.getLogger(__name__)


class FailedNotamRetryService:
    def __init__(self, max_retry_attempts: int = 3, retry_delay_hours: int = 1):
        self.max_retry_attempts = max_retry_attempts
        self.retry_delay_hours = retry_delay_hours

    def get_retry_stats(self) -> Dict[str, int]:
        """Get statistics about failed NOTAMs"""
        session = SessionLocal()
        try:
            total = session.query(FailedNotam).count()
            pending = session.query(FailedNotam).filter(
                FailedNotam.retry_count < self.max_retry_attempts
            ).count()
            exhausted = total - pending

            return {
                'total': total,
                'pending': pending,
                'exhausted': exhausted
            }
        finally:
            session.close()


    async def retry_failed_notams(self, batch_size: int = 20):
        """Main retry logic with stats"""
        stats = self.get_retry_stats()
        if stats['pending'] == 0:
            if stats['total'] > 0:
                log.info("✅ No failed NOTAMs ready for retry (%d exhausted)", stats['exhausted'])
            return

        failed_items = self.get_failed_notams_for_retry(limit=batch_size)
        if not failed_items:
            return

        log.info("🔄 Auto-retrying %d failed NOTAMs (%d pending total)...",
                 len(failed_items), stats['pending'])

        # Analyze with gentler settings for retries
        results = await analyze_many(
            failed_items,
            max_concurrency=5,  # Even lower for auto-retry
            rps=1.0,  # Very gentle rate for auto-retry
            timeout_sec=240.0,  # Longer timeout
            retry_attempts=1  # Single retry attempt
        )

        # Save results
        save_results_batch(results)

        # Clean up successful retries
        successful_hashes = [
            r["input"]["raw_hash"] for r in results
            if r["result"] is not None
        ]
        self.cleanup_successful_retries(successful_hashes)

        success_count = len(successful_hashes)
        fail_count = len(results) - success_count

        # Log results with updated stats
        new_stats = self.get_retry_stats()
        log.info("🎯 Auto-retry complete: %d succeeded, %d failed (%d still pending)",
                 success_count, fail_count, new_stats['pending'])

# CLI function
async def retry_failed_notams_cli():
    service = FailedNotamRetryService()
    await service.retry_failed_notams()


if __name__ == "__main__":
    import sys
    import logging

    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    asyncio.run(retry_failed_notams_cli())