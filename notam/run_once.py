# notam/run_once.py
import os
import logging, sys
from pathlib import Path
from notam.pipeline import run_pipeline

# --- print-style logging to console ---
logging.basicConfig(
    level=logging.INFO,         # set DEBUG for more detail
    format="%(message)s",       # looks like print()
    stream=sys.stdout,
    force=True,
)
# Quiet noisy libraries a bit (optional)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("langchain").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

def default_csv_path() -> Path:
    # project root = parent of the 'notam' package
    root = Path(__file__).resolve().parents[1]
    return root / "data" / "Airport Database - NOTAM ID.csv"

def _truthy(val: str | None) -> bool:
    """Turn env var strings like '1', 'true', 'yes' into True."""
    return (val or "").strip().lower() in {"1", "true", "yes", "y", "on"}

def _parse_int_list(val: str | None) -> list[int]:
    """Turn '2917 3021,3055' into [2917, 3021, 3055]."""
    if not val:
        return []
    toks = val.replace(",", " ").split()
    out: list[int] = []
    for t in toks:
        try:
            out.append(int(t))
        except ValueError:
            log.warning("Ignoring non-integer NOTAM id: %r", t)
    return out

if __name__ == "__main__":
    # allow override by env var if you want:
    #   PowerShell: $env:NOTAM_CSV="C:\full\path.csv"
    csv_from_env = os.getenv("NOTAM_CSV")
    csv_path = Path(csv_from_env) if csv_from_env else default_csv_path()

    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV not found at:\n  {csv_path}\n"
            "Tip: put the file in <project>/data/ or set NOTAM_CSV to a full path."
        )

    log.info("Using CSV: %s", csv_path)

    # NEW: overwrite modes via env
    #   NOTAM_OVERWRITE_ALL=1                -> TRUNCATE CASCADE on first save
    #   NOTAM_OVERWRITE_DB_IDS="2917 3021"   -> delete these ids before first save
    overwrite_all = _truthy(os.getenv("NOTAM_OVERWRITE_ALL"))
    overwrite_db_ids = _parse_int_list(os.getenv("NOTAM_OVERWRITE_DB_IDS"))
    only_overwrite_ids = _truthy(os.getenv("NOTAM_ONLY_OVERWRITE_IDS"))

    try:
        # keep 'overwrite' (legacy clear_db) defaulting to False
        run_pipeline(
            csv_path=str(csv_path),
            overwrite=False,
            overwrite_all=overwrite_all,
            overwrite_db_ids=overwrite_db_ids or None,
            only_overwrite_ids=only_overwrite_ids,
            max_concurrency=300,
        )
        log.info("Done.")
    except Exception:
        logging.exception("Run failed")
        raise

#Usage

##$env:NOTAM_OVERWRITE_ALL="1"
##python -m notam.run_once


#$env:NOTAM_ONLY_OVERWRITE_IDS="1"
##$env:NOTAM_OVERWRITE_DB_IDS="2918 3021"
##python -m notam.run_once