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
    # Load manual NOTAMs from separate file
    manual_path = csv_path.replace('NOTAM ID.csv', 'Manual NOTAMs.csv')
    manual_notams_by_airport = {}

    try:
        df_manual = pd.read_csv(manual_path)
        for _, row in df_manual.iterrows():
            airport = str(row["airport_code"]).strip().upper()
            notam_num = str(row["notam_number"]).strip()
            message = str(row["message"]).strip()

            if airport and notam_num and message:
                if airport not in manual_notams_by_airport:
                    manual_notams_by_airport[airport] = []

                manual_notams_by_airport[airport].append({
                    "issue_time": None,  # Let AI extract from message
                    "notam_number": notam_num,
                    "icao_message": message,
                    "airport": airport,
                    "url": "MANUAL_CSV",
                })

        log.info("📋 Loaded manual NOTAMs for %d airports", len(manual_notams_by_airport))
    except FileNotFoundError:
        log.info("No manual NOTAMs file found")
    except Exception as e:
        log.warning("Could not load manual NOTAMs: %s", e)

    # Load main airport database
    df = pd.read_csv(csv_path, usecols=["Designator", "URL"])
    df = df.dropna(how="all", subset=["Designator", "URL"])
    df = df[~(
            (df["Designator"].astype(str).str.strip() == "") &
            (df["URL"].astype(str).str.strip() == "")
    )].reset_index(drop=True)

    log.info("🔗 Processing %d airports...", len(df))
    notam_objs = []

    for _, row in df.iterrows():
        designator = str(row["Designator"]).strip()
        url = str(row["URL"]).strip()

        notams_for_airport = []

        # Try URL first if available
        if url.lower() not in ["", "nan"]:
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
                            notams_for_airport.append({
                                "issue_time": date,
                                "notam_number": str(num).strip(),
                                "icao_message": str(msg).strip(),
                                "airport": designator,
                                "url": url,
                            })
                            count += 1
                    log.info("✅ URL: %d NOTAMs for %s", count, designator)
                else:
                    log.error("❗ HTTP %s for %s", resp.status_code, designator)
            except Exception:
                log.warning("❌ URL failed for %s", designator)

        # Fallback to manual NOTAMs if URL failed or missing
        if not notams_for_airport and designator in manual_notams_by_airport:
            notams_for_airport = manual_notams_by_airport[designator]
            log.info("📋 Manual: %d NOTAMs for %s", len(notams_for_airport), designator)

        # No NOTAMs found at all
        if not notams_for_airport:
            log.warning("⚠️ No NOTAMs found for %s", designator)

        notam_objs.extend(notams_for_airport)

    log.info("📊 Total: %d NOTAMs loaded", len(notam_objs))
    return notam_objs