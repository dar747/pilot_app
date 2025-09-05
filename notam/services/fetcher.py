# notam/services/fetcher.py
import logging
import pandas as pd
import requests

log = logging.getLogger(__name__)

def _get_with_backoff(url, headers=None, attempts=4, timeout=15, base=0.5):
    import time, random
    headers = headers or {}
    for i in range(attempts):
        try:
            return requests.get(url, headers=headers, timeout=timeout)
        except requests.RequestException as e:
            if i == attempts - 1:
                raise
            sleep = base * (2 ** i) + random.random() * 0.2
            log.warning(
                "HTTP error fetching %s (try %d/%d): %s; retrying in %.2fs",
                url, i + 1, attempts, e, sleep
            )
            time.sleep(sleep)

def fetch_notam_data_from_csv(csv_path: str):
    df = pd.read_csv(csv_path, usecols=["Designator", "URL"])
    df = df.dropna(how="all", subset=["Designator", "URL"])
    df = df[~(
        (df["Designator"].astype(str).str.strip() == "") &
        (df["URL"].astype(str).str.strip() == "")
    )].reset_index(drop=True)

    log.info("🔗 %d links to fetch…", len(df))
    notam_objs = []

    for _, row in df.iterrows():
        designator = str(row["Designator"]).strip()
        url = str(row["URL"]).strip()
        if url.lower() in ["", "nan"]:
            log.warning("Skipping %s due to missing URL", designator)
            continue

        log.info("📡 Fetching %s: %s", designator, url)
        try:
            resp = _get_with_backoff(
                url,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                timeout=15
            )
            if resp.status_code == 200:
                data = resp.json()
                count = 0
                for n in data.get("notams", []):
                    msg = n.get("icaoMessage")
                    num = n.get("notamNumber")
                    date = n.get("issueDate")
                    if msg and num and msg.strip():
                        notam_objs.append({
                            "issue_time": date,
                            "notam_number": str(num).strip(),
                            "icao_message": str(msg).strip(),
                            "airport": designator,
                            "url": url,
                        })
                        count += 1
                log.info("✅ Stored %d NOTAMs for %s", count, designator)
            else:
                log.error("❗ HTTP %s for %s (%s): %s",
                          resp.status_code, designator, url, resp.text[:300])
        except Exception:
            log.exception("❌ Error fetching %s from %s", designator, url)

    return notam_objs
