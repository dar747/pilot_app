# scripts/daily_scheduler.py  (or wherever you run it)
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from logging.handlers import RotatingFileHandler
from pathlib import Path
import logging, os, sys

from notam.pipeline import run_pipeline

# --- logging: console + rotating file ---
logs_dir = Path(__file__).resolve().parents[1] / "logs"
logs_dir.mkdir(parents=True, exist_ok=True)
log_path = logs_dir / "daily_scheduler.log"

root = logging.getLogger()
root.setLevel(logging.INFO)
fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")

ch = logging.StreamHandler(sys.stdout); ch.setFormatter(fmt)
fh = RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=5, encoding="utf-8"); fh.setFormatter(fmt)
root.handlers = [ch, fh]
log = logging.getLogger(__name__)

# --- config ---
CSV_PATH = os.getenv("NOTAM_CSV") or str(Path(__file__).resolve().parents[1] / "data" / "Airport Database - NOTAM ID.csv")

# Optional hard guard: fail fast if overwrite envs are present in prod
forbidden_envs = ["NOTAM_OVERWRITE_ALL", "NOTAM_OVERWRITE_DB_IDS", "NOTAM_ONLY_OVERWRITE_IDS"]
bad = [k for k in forbidden_envs if os.getenv(k)]
if bad:
    raise RuntimeError(f"Refusing to start: overwrite env var(s) set in production: {bad}")

def daily_job():
    try:
        log.info("🛫 Starting NOTAM ingestion (incremental, no overwrite)")
        run_pipeline(
            csv_path=CSV_PATH,
            overwrite=False,              # legacy: never wipe
            overwrite_all=False,          # never TRUNCATE
            overwrite_db_ids=None,        # never target-delete
            only_overwrite_ids=False,     # never strict mode
            max_concurrency=80,           # tune for your infra
        )
        log.info("✅ Daily NOTAM job completed.")
    except Exception:
        log.exception("❌ Daily job failed")

if __name__ == "__main__":
    # UTC, coalesce missed runs into one, allow 10 min misfire grace, single instance at a time
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        daily_job,
        trigger=CronTrigger(hour=3, minute=0),   # run daily at 03:00 UTC
        id="daily_notam_ingest",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=600,                  # seconds
        max_instances=1,
    )
    log.info("Scheduler is running (UTC)… Ctrl+C to stop. Log file: %s", log_path)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped.")
