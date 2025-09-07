# notam/retry_failed_notams.py
import asyncio
import logging
import sys
from dotenv import load_dotenv
from notam.db import init_db
from notam.services.retry_failed import retry_failed_notams_cli

if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    init_db()
    asyncio.run(retry_failed_notams_cli())