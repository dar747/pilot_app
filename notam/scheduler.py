import pandas as pd
import requests
import asyncio
import hashlib
from typing import List, Dict
from notam.analyze import analyze_notam
from notam.db import NotamRecord, SessionLocal, init_db
import json
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent  # Goes up from notam/ to project root


def build_and_populate_db(overwrite=False):
    init_db()
    csv_path = BASE_DIR / "data" / "Airport Database - NOTAM ID.csv"
    all_notams = fetch_notam_data_from_csv(str(csv_path))
    if overwrite:
        clear_db()
        existing_hashes = set()
    else:
        existing_hashes = get_existing_hashes()

    to_analyze = []
    seen_in_run = set()  # NEW

    for n in all_notams:
        h = get_hash(n["notam_number"], n["icao_message"])
        print(f"NOTAM: {n['notam_number']}, Hash: {h}")  # <-- This line prints the hash
        if h in existing_hashes or h in seen_in_run:  # MODIFIED
            print(f"⏩ Already in DB or batch, skipping {n['notam_number']} | hash: {h}")
        else:
            n["raw_hash"] = h
            to_analyze.append(n)
            seen_in_run.add(h)  # NEW
            print(f"🆕 Will analyze: {n['notam_number']} | hash: {h}")

    print(f"✅ {len(to_analyze)} new NOTAMs to analyze")

    if not to_analyze:
        print("🎉 All NOTAMs already analyzed and stored. Exiting.")
    else:
        asyncio.run(run_analysis(to_analyze, batch_size=800))



def fetch_notam_data_from_csv(csv_path: str) -> List[Dict]:
    df = pd.read_csv(csv_path, usecols=['Designator', 'URL'])
    df = df.dropna(how='all', subset=['Designator', 'URL'])
    df = df[~(
        (df['Designator'].astype(str).str.strip() == '') &
        (df['URL'].astype(str).str.strip() == '')
    )].reset_index(drop=True)

    print(f"🔗 {len(df)} links to fetch…")
    notam_objs = []

    for _, row in df.iterrows():
        designator = str(row['Designator']).strip()
        url = str(row['URL']).strip()
        if url.lower() in ["", "nan"]:
            print(f"⚠️  Skipping {designator} due to missing URL")
            continue

        print(f"📡 Fetching {designator}: {url}")
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                timeout=10
            )
            if resp.status_code == 200:
                data = resp.json()
                for n in data.get("notams", []):
                    msg = n.get("icaoMessage")
                    num = n.get("notamNumber")
                    date = n.get("issueDate")
                    if msg and num and msg.strip():
                        notam_objs.append({
                            "issue_time": date,
                            "notam_number": num.strip(),
                            "icao_message": msg.strip(),
                            "airport": designator,
                            "url":url
                        })
                print(f"✅ Stored {len(data.get('notams', []))} NOTAMs")
            else:
                print(f"❗ HTTP {resp.status_code} for {designator}")
        except requests.RequestException as e:
            print(f"❌ Error fetching {designator}: {e}")

    return notam_objs

def get_hash(notam_number, icao_message):
    combined = f"{notam_number.strip()}|{icao_message.strip()}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()

def get_existing_hashes():
    session = SessionLocal()
    hashes = set(x[0] for x in session.query(NotamRecord.raw_hash).all() if x[0])
    session.close()
    print(f"🔎 DB contains {len(hashes)} existing NOTAM hashes.")
    return hashes

def clear_db():
    session = SessionLocal()
    try:
        num_deleted = session.query(NotamRecord).delete()
        session.commit()
        print(f"🧹 Cleared {num_deleted} NOTAM records from the database.")
    except Exception as e:
        session.rollback()
        print(f"❌ Failed to clear database: {e}")
    finally:
        session.close()


def save_to_db(result, raw_text, notam_number, raw_hash, airport):
    session = SessionLocal()
    try:
        exists = session.query(NotamRecord).filter(
            (NotamRecord.airport == airport) &
            (NotamRecord.notam_number == notam_number)
        ).first()

        if exists:
            print(f"⏩ Skipping duplicate NOTAM {notam_number} at {airport}")
            return

        record = NotamRecord(
            airport=airport,
            notam_number=notam_number,
            issue_time=result.issue_time,
            notam_info_type=result.notam_info_type,
            notam_category=result.notam_category,
            start_time=result.start_time,
            end_time=result.end_time,
            seriousness=result.seriousness,
            applied_scenario=result.applied_scenario,
            applied_aircraft_type=result.applied_aircraft_type,
            operational_tag=",".join(result.operational_tag) if isinstance(result.operational_tag, list) else result.operational_tag,
            affected_runway=",".join(result.affected_runway) if isinstance(result.affected_runway, list) else result.affected_runway,
            notam_summary=result.notam_summary,
            icao_message=raw_text,
            replacing_notam=result.replacing_notam,
            raw_hash=raw_hash
        )
        session.add(record)
        session.commit()
        print(f"📝 Saved {result.notam_number} at {airport}")
    except Exception as e:
        session.rollback()
        print(f"❌ DB error: {e}")
    finally:
        session.close()

async def run_analysis(to_analyze: List[Dict], batch_size=200):
    print(f"📦 Running analysis on {len(to_analyze)} new NOTAMs...")
    for i in range(0, len(to_analyze), batch_size):
        batch = to_analyze[i:i+batch_size]
        tasks = [analyze_notam(n["icao_message"],n["issue_time"]) for n in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for j, result in enumerate(results):
            notam_number = batch[j]["notam_number"]
            icao_message = batch[j]["icao_message"]
            airport = batch[j].get("airport", "Unknown")
            url = batch[j].get("url", "Unknown")
            if isinstance(result, Exception) or result is None:
                print(f"❌ Error analyzing NOTAM {notam_number}: {result}")
            else:
                print(f"\n--- ICAO Message for NOTAM {notam_number} ---\n{icao_message}\n")
                print(f"🔗 Source URL: {url}")
                print(f"📊 Analysis Result for {notam_number} ({airport}):\n{json.dumps(result.model_dump(), indent=2)}\n")

                save_to_db(
                    result,
                    icao_message,
                    notam_number,
                    batch[j]["raw_hash"],
                    airport
                )


if __name__ == "__main__":
    build_and_populate_db()



