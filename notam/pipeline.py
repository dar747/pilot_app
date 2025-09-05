# notam/pipeline.py
import logging
from typing import List, Dict, Optional

from notam.db import init_db
from notam.services.fetcher import fetch_notam_data_from_csv
from notam.services.analyser import analyze_many
from notam.services.persistence import (
    get_existing_hashes,
    get_hash,
    save_results_batch,
    clear_db,
    get_raw_hashes_for_notam_ids,   # used to force-include specific ids
)

log = logging.getLogger(__name__)


def run_pipeline(
    *,
    csv_path: str,
    overwrite: bool = False,
    overwrite_all: bool = False,                      # wipe all via TRUNCATE CASCADE on first save
    overwrite_db_ids: Optional[List[int]] = None,     # delete specific DB ids on first save
    only_overwrite_ids: bool = False,                 # strict mode: analyze only the ids passed
    # Pass 1 (faster)
    max_concurrency: int = 80,
    rps_first: float = 8.0,
    timeout_sec: float = 120.0,
    retry_attempts: int = 1,                          # per item inside pass 1
    # Pass 2 (gentler retry)
    retry_concurrency: int = 16,
    rps_retry: float = 3.0,
    retry_timeout_sec: float = 300.0,
    retry_attempts_pass2: int = 2,
) -> None:
    """
    End-to-end pipeline:
      - read CSV
      - analyze NOTAMs concurrently (2-pass with retries)
      - persist to DB (with optional overwrite modes)

    Overwrite modes:
      overwrite=True            -> call clear_db() (legacy full delete)
      overwrite_all=True        -> fast TRUNCATE CASCADE (handled inside save_results_batch)
      overwrite_db_ids=[...]    -> targeted delete by NotamRecord.id (handled inside save_results_batch)

    Selection modes:
      only_overwrite_ids=False  -> analyze NEW items ∪ FORCED ids (default)
      only_overwrite_ids=True   -> analyze ONLY the FORCED ids
    """
    init_db()

    all_notams: List[Dict] = fetch_notam_data_from_csv(csv_path)
    if not all_notams:
        log.info("No NOTAMs found in CSV.")
        return

    # Decide how to dedupe before analysis
    if overwrite:
        # Legacy nuke: delete via ORM bulk delete
        clear_db()
        existing_hashes = set()
    elif overwrite_all:
        # Full refresh analyzes everything (skip DB-based dedupe)
        existing_hashes = set()
    else:
        existing_hashes = get_existing_hashes()

    # If targeting specific DB ids, compute the corresponding hashes so we force-include them
    forced_hashes = set()
    if overwrite_db_ids:
        try:
            forced_hashes = get_raw_hashes_for_notam_ids(overwrite_db_ids)
        except Exception:
            # Don't fail the whole run just because this lookup had an issue
            log.exception("Failed to fetch raw_hashes for overwrite_db_ids=%s", overwrite_db_ids)
            forced_hashes = set()

    # Build the analysis queue
    to_analyze: List[Dict] = []
    seen_in_run = set()

    for n in all_notams:
        h = get_hash(n["notam_number"], n["icao_message"])

        if only_overwrite_ids:
            # strict mode: include only forced hashes; avoid dupes in this run
            if h not in forced_hashes or h in seen_in_run:
                continue
        else:
            # default: include NEW items or FORCED items; avoid dupes in this run
            if (h in existing_hashes and h not in forced_hashes) or (h in seen_in_run):
                continue

        n["raw_hash"] = h
        to_analyze.append(n)
        seen_in_run.add(h)

    log.info(
        "✅ %d NOTAMs to analyze (existing_hashes=%d, forced_hashes=%d, only_overwrite_ids=%s)",
        len(to_analyze), len(existing_hashes), len(forced_hashes), only_overwrite_ids
    )
    if not to_analyze:
        return

    import asyncio  # keep local to avoid event loop surprises for embedders

    # -------- Pass 1 --------
    log.info(
        "Pass 1: conc=%d rps=%s timeout=%ss",
        max_concurrency,
        ("∞" if rps_first <= 0 else rps_first),
        timeout_sec,
    )
    results1 = asyncio.run(
        analyze_many(
            to_analyze,
            max_concurrency=max_concurrency,
            rps=rps_first,
            timeout_sec=timeout_sec,
            retry_attempts=retry_attempts,
        )
    )

    # Persist first pass; apply overwrite modes ONCE here
    save_results_batch(
        results1,
        overwrite_all=overwrite_all,
        overwrite_db_ids=overwrite_db_ids,
    )

    # Collect failures for a gentler retry pass
    fail_items = [r["input"] for r in results1 if r["result"] is None]
    if not fail_items:
        return

    log.info("Retrying %d failed NOTAMs with lower pressure…", len(fail_items))

    # -------- Pass 2 (gentle) --------
    results2 = asyncio.run(
        analyze_many(
            fail_items,
            max_concurrency=retry_concurrency,
            rps=rps_retry,
            timeout_sec=retry_timeout_sec,
            retry_attempts=retry_attempts_pass2,
        )
    )

    # Do NOT pass overwrite flags again; we only wipe/delete once, above
    save_results_batch(results2)

    still_failed = [r for r in results2 if r["result"] is None]
    if still_failed:
        log.warning("%d NOTAMs still failed after retries (skipped).", len(still_failed))
