# in notam/airports_sync_min.py (or wherever you ingest airports)

from datetime import datetime
from notam.db import Airport  # updated class above

def parse_iso_or_none(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def upsert_airport(session, a):
    icao = (a.get("icaoId") or "").strip().upper()
    if not icao:
        return None

    ap = session.get(Airport, icao)
    if not ap:
        ap = Airport(icao_code=icao)
        session.add(ap)

    ap.iata_code = (a.get("iataId") or None)
    ap.faa_id = (a.get("faaId") or None)
    ap.name = (a.get("name") or None)
    ap.country = (a.get("country") or None)

    ap.lat = a.get("lat")
    ap.lon = a.get("lon")
    ap.elev = a.get("elev")

    # Optional/extras if your fetcher provides them; else leave None
    ap.freqs = a.get("freqs")  # JSON
    ap.timezone = a.get("timezone")  # string like "Asia/Hong_Kong"
    ap.utc_offset_normal = a.get("utc_offset_normal")  # hours
    ap.utc_offset_dst = a.get("utc_offset_dst")        # hours
    ap.changetodst = parse_iso_or_none(a.get("changetodst"))
    ap.changefromdst = parse_iso_or_none(a.get("changefromdst"))
    ap.magnetic_declination = (a.get("magdec") or a.get("magnetic_declination") or None)

    return ap
