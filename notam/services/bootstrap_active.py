# notam/services/bootstrap_active.py
import os, logging, time
from typing import Dict, List
import requests

log = logging.getLogger(__name__)

def _env(name: str, default: str | None = None, required: bool = False) -> str | None:
    v = os.getenv(name, default)
    if required and not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def fetch_active_notams_via_rest(airports: List[str]) -> List[Dict]:
    """
    Fetches *currently active* NOTAMs via a REST API and returns your pipeline items:
      { issue_time, notam_number, icao_message, airport, url }
    This is intentionally generic; plug in your official FNS REST endpoint below.
    """
    base = _env("NOTAM_REST_BASE", required=True).rstrip("/")
    api_key = _env("NOTAM_REST_API_KEY")  # if your endpoint needs a key/token
    headers = {
        "Accept": "application/json",
        "User-Agent": "notam-bootstrap/1.0",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    items: List[Dict] = []
    for icao in airports:
        icao = icao.strip().upper()
        if not icao:
            continue

        # EXAMPLE URL pattern — adapt to your official endpoint.
        # Many FNS-style endpoints accept designator and active filter.
        # e.g. GET {base}/notams?designator={ICAO}&activeOnly=true
        url = f"{base}/notams?designator={icao}&activeOnly=true"

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code != 200:
                log.warning("REST %s for %s: %s", resp.status_code, icao, resp.text[:300])
                continue

            data = resp.json() or {}
            # Common shapes seen in FNS-style responses:
            #  {
            #    "notams": [
            #      {"icaoMessage":"...", "notamNumber":"A1234/24", "issueDate":"2025-09-04T12:34:56Z", ...},
            #      ...
            #    ]
            #  }
            rows = data.get("notams", data if isinstance(data, list) else [])
            added = 0
            for n in rows:
                msg = (n.get("icaoMessage") or n.get("text") or "").strip()
                num = (n.get("notamNumber") or n.get("number") or "").strip()
                issued = (n.get("issueDate") or n.get("issueTime") or n.get("issued") or "").strip()
                if not msg:
                    continue
                if not num:
                    # try to derive like “A1234/24”
                    import re
                    m = re.search(r"\b[A-Z]?\d{3,5}/\d{2}\b", msg)
                    num = m.group(0) if m else f"UNK-{int(time.time()*1000)}"
                items.append({
                    "issue_time": issued or None,
                    "notam_number": num,
                    "icao_message": msg,
                    "airport": icao,
                    "url": url,
                })
                added += 1
            log.info("Fetched %d active NOTAM(s) for %s", added, icao)
        except Exception as e:
            log.exception("Error fetching %s from %s", icao, url)
    return items
