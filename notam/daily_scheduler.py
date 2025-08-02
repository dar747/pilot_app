# daily_scheduler.py

from apscheduler.schedulers.blocking import BlockingScheduler
from notam.scheduler import build_and_populate_db
from notam.push_to_supabase import push_new_to_supabase
import logging
import os

# Set up logging
log_dir = "../logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"{log_dir}/notam_daily.log"),
        logging.StreamHandler()  # console output
    ]
)

log = logging.getLogger(__name__)

# Define the job
def daily_job():
    try:
        log.info("🛫 Starting NOTAM analysis and sync...")
        build_and_populate_db(overwrite=False)
        push_new_to_supabase(overwrite=False)
        log.info("✅ Daily NOTAM job completed.")
    except Exception as e:
        log.exception("❌ Daily job failed with exception:")

# Schedule the job
if __name__ == "__main__":
    scheduler = BlockingScheduler()
    #scheduler.add_job(daily_job, trigger='cron', hour=3, minute=0)  # Daily at 03:00 AM
    scheduler.add_job(daily_job, trigger='interval', minutes=30)
    log.info("Scheduler is running... (Ctrl+C to stop)")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("👋 Scheduler stopped manually.")
