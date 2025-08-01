from db import SessionLocal, NotamRecord
import hashlib

def get_hash(notam_number,icao_message):
    combined = f"{notam_number.strip()}|{icao_message.strip()}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()

def populate_hashes():
    session = SessionLocal()
    updated = 0
    seen_hashes = set()
    notams = session.query(NotamRecord).all()
    # Pre-load hashes to avoid IntegrityError
    for n in notams:
        if n.raw_hash:
            seen_hashes.add(n.raw_hash)
    for n in notams:
        # Always recalculate the hash, regardless of existing value
        if n.icao_message and n.icao_message.strip():
            h = get_hash(n.notam_number, n.icao_message)
            # Only set and update if different (for database efficiency)
            if n.raw_hash != h:
                if h in seen_hashes:
                    print(f"⚠️ Duplicate raw_hash detected, skipping: {n.notam_number} / {n.airport}")
                    continue
                n.raw_hash = h
                seen_hashes.add(h)
                updated += 1
    session.commit()
    session.close()
    print(f"✅ Updated {updated} records with raw_hash.")

if __name__ == "__main__":
    populate_hashes()
