# notam/services/run_swim.py
import logging, sys
from dotenv import load_dotenv
from notam.db import init_db
from notam.services.swim_consumer import main as swim_main

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout, force=True)
    load_dotenv()  # loads .env from CWD
    init_db()
    swim_main()
