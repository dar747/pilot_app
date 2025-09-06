# notam/services/swim_consumer.py
import os
import sys
import signal
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
import threading, time
from notam.core.repository import NotamRepository
from notam.services.analyser import analyze_many

from solace.messaging.messaging_service import MessagingService, RetryStrategy
from solace.messaging.receiver.persistent_message_receiver import PersistentMessageReceiver
from solace.messaging.receiver.message_receiver import MessageReceiver
from solace.messaging.resources.queue import Queue

# Property modules + auth (PubSub+ Python SDK 1.10)
from solace.messaging.config.solace_properties import (
    transport_layer_properties as tlp,  # HOST, RECONNECTION_*
    service_properties as sp,  # VPN_NAME
    transport_layer_security_properties as tls_p  # CERT_*, TRUST_STORE_PATH, ...
)
from solace.messaging.config.authentication_strategy import BasicUserNamePassword

# Import MessageHandler with fallback for different SDK versions
try:
    from solace.messaging.receiver.message_receiver import MessageHandler

    HAS_MESSAGE_HANDLER = True
except ImportError:
    # Fallback for different SDK versions
    MessageHandler = None
    HAS_MESSAGE_HANDLER = False

log = logging.getLogger(__name__)


def _env(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    v = os.getenv(name, default)
    if required and not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_notam_fields(payload: str) -> Dict[str, Optional[str]]:
    """
    Parse FAA AIM FNS payloads.
    Tries JSON first, then AIXM XML (with namespace-aware <pre> extraction).
    Returns keys: icao_message, notam_number, issue_time, airport.
    """
    payload = payload or ""

    # JSON path
    try:
        import json
        j = json.loads(payload)

        maybe = (
                j.get("icaoMessage")
                or (isinstance(j.get("notam"), dict) and j["notam"].get("icaoMessage"))
                or j.get("TextNOTAM")
        )
        num = j.get("notamNumber") or (j.get("notam") or {}).get("notamNumber") or j.get("NotamNumber")
        issued = j.get("issueDate") or j.get("issueTime") or j.get("IssueTime")
        ap = j.get("location") or j.get("stationId") or j.get("Designator") or j.get("Airport")

        if maybe:
            return {
                "icao_message": str(maybe),
                "notam_number": (str(num).strip() if num else None),
                "issue_time": issued or _now_iso(),
                "airport": (str(ap).upper() if ap else "UNKNOWN"),
            }
    except Exception:
        pass

    # XML (AIXM 5.1) path
    try:
        import xml.etree.ElementTree as ET
        from html import unescape

        root = ET.fromstring(payload)

        ns = {
            "gml": "http://www.opengis.net/gml/3.2",
            "aixm": "http://www.aixm.aero/schema/5.1",
            "event": "http://www.aixm.aero/schema/5.1/event",
            "msg": "http://www.aixm.aero/schema/5.1/message",
            "fnse": "http://www.aixm.aero/schema/5.1/extensions/FAA/FNSE",
            "html": "http://www.w3.org/1999/xhtml",
        }

        # Prefer ICAO formatted <pre> inside html:div (escaped)
        icao_msg = ""
        for div in root.findall(".//html:div", ns):
            txt = "".join(div.itertext()).strip()
            if txt:
                txt = unescape(txt)  # contains <pre> … </pre>
                if "<pre>" in txt:
                    icao_msg = txt.split("<pre>", 1)[1].split("</pre>", 1)[0].strip()
                else:
                    icao_msg = txt.strip()
                if icao_msg:
                    break

        # Fallback: plain text field in event:NOTAM
        if not icao_msg:
            tnode = root.find(".//event:textNOTAM/event:NOTAM/event:text", ns)
            if tnode is not None and (tnode.text or "").strip():
                icao_msg = tnode.text.strip()

        # Build NOTAM number: series + number + year => e.g. X6073/25
        series = (root.findtext(".//event:NOTAM/event:series", default="", namespaces=ns) or "").strip()
        number = (root.findtext(".//event:NOTAM/event:number", default="", namespaces=ns) or "").strip()
        year = (root.findtext(".//event:NOTAM/event:year", default="", namespaces=ns) or "").strip()
        notam_no = f"{series}{number}/{year[-2:]}" if (series and number and year) else None

        issued = (root.findtext(".//event:NOTAM/event:issued", default="", namespaces=ns) or "").strip()
        ap = (root.findtext(".//event:NOTAM/event:location", default="", namespaces=ns) or "").strip()

        if icao_msg:
            return {
                "icao_message": icao_msg,
                "notam_number": notam_no,
                "issue_time": issued or _now_iso(),
                "airport": (ap or "UNKNOWN").upper(),
            }
    except Exception:
        pass

    # Fallback: raw
    return {
        "icao_message": payload.strip(),
        "notam_number": None,
        "issue_time": _now_iso(),
        "airport": "UNKNOWN",
    }


def _ack_message(msg):
     """For this Solace SDK version, acknowledgment is automatic for persistent receivers"""
     return True


class SwimConsumer:
    def __init__(self):
        self.repository = NotamRepository()
        # Required connection bits
        self.host = _env("SWIM_HOST", required=True)  # e.g. tcps://ems1.swim.faa.gov:55443
        self.vpn = _env("SWIM_VPN", required=True)  # AIM_FNS
        self.user = _env("SWIM_USERNAME", required=True)
        self.pw = _env("SWIM_PASSWORD", required=True)
        self.queue = _env("SWIM_QUEUE", required=True)
        self.trust = _env("SWIM_TRUST_STORE_PEM", required=True)  # DIRECTORY with PEM/CRT

        # Micro-batch tuning (small for quick feedback)
        self.batch_size = int(_env("SWIM_BATCH_SIZE", "3"))
        self.batch_secs = float(_env("SWIM_BATCH_INTERVAL_SEC", "2"))
        self.max_inflight = int(_env("SWIM_MAX_INFLIGHT", "500"))

        # Internals
        self._msgs: List[Dict] = []
        self._receiver: Optional[PersistentMessageReceiver] = None
        self._svc: Optional[MessagingService] = None
        self._stop = False

        # Load airports from CSV (use default path if not specified)
        csv_path = _env("SWIM_AIRPORT_CSV_PATH", "")

        from notam.services.airport_config import load_monitored_airports

        if csv_path:
            self.airports_filter = load_monitored_airports(csv_path)
        else:
            # Use default CSV path
            self.airports_filter = load_monitored_airports()

        if self.airports_filter:
            log.info("🎯 Monitoring %d airports from CSV", len(self.airports_filter))
        else:
            log.info("🌍 No airports loaded - monitoring all airports")

    def connect(self):
        # Normalize trust path: SDK expects a DIRECTORY
        trust_raw = (self.trust or "").strip().strip('"')
        trust_dir = os.path.dirname(trust_raw) if os.path.isfile(trust_raw) else trust_raw

        props = {
            # Connection + VPN
            tlp.HOST: self.host,
            sp.VPN_NAME: self.vpn,

            # TLS validation via properties (no TLS helper in 1.10)
            tls_p.CERT_VALIDATED: True,
            tls_p.CERT_VALIDATE_SERVERNAME: True,
            tls_p.TRUST_STORE_PATH: trust_dir,

            # Reliability knobs
            tlp.RECONNECTION_ATTEMPTS: -1,
            tlp.RECONNECTION_ATTEMPTS_WAIT_INTERVAL: 3000,  # ms
        }

        self._svc = (
            MessagingService.builder()
            .from_properties(props)
            .with_authentication_strategy(BasicUserNamePassword(self.user, self.pw))
            .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(1000, 3000))
            .build()
        )

        # Optional: service events
        try:
            def _svc_evt(evt):
                print("Service event:", getattr(evt, "event_name", evt))

            self._svc.set_service_event_listener(_svc_evt)  # may not exist in older builds
        except Exception:
            pass

        self._svc.connect()
        log.info("Connected to SWIM broker")

        q = Queue.durable_exclusive_queue(self.queue)
        self._receiver = self._svc.create_persistent_message_receiver_builder().build(q)
        self._receiver.start()
        log.info("Receiver started on queue: %s", self.queue)

    def run(self):


        inflight_lock = threading.Lock()
        last_flush = time.monotonic()
        msg_count = 0

        def handle(msg):
            nonlocal last_flush, msg_count
            msg_count += 1

            try:
                payload = msg.get_payload_as_string() or ""

                fields = _extract_notam_fields(payload)

                if not fields.get("icao_message"):
                    _ack_message(msg)
                    return

                # ADD AIRPORT FILTERING HERE - before creating the item
                airport = fields.get("airport", "UNKNOWN").upper()
                if self.airports_filter and airport not in self.airports_filter:
                    _ack_message(msg)  # Skip - not in monitored airports
                    return

                # derive a notam_number if missing (best-effort)
                if not fields.get("notam_number"):
                    import re
                    m = re.search(r"\b[A-Z]?\d{3,5}/\d{2}\b", fields["icao_message"])
                    if m:
                        fields["notam_number"] = m.group(0)

                item = {
                    "issue_time": fields["issue_time"],
                    "notam_number": (fields.get("notam_number") or f"UNK-{int(time.time() * 1000)}"),
                    "icao_message": fields["icao_message"],
                    "airport": fields.get("airport") or "UNKNOWN",
                    "url": "SWIM:AIM_FNS",
                }
                item["raw_hash"] = self.repository.get_hash(item["notam_number"], item["icao_message"])

                with inflight_lock:
                    self._msgs.append(item)

                _ack_message(msg)

            except Exception as e:
                log.exception("Message handling error: %s", e)
                _ack_message(msg)

        # Wrap function in required MessageHandler type (with SDK version compatibility)
        if HAS_MESSAGE_HANDLER and MessageHandler:
            # SDK version with MessageHandler class
            class _Handler(MessageHandler):
                def __init__(self, fn): self._fn = fn

                def on_message(self, inbound_msg): self._fn(inbound_msg)

            handler = _Handler(handle)
        else:
            # SDK version without MessageHandler class - try direct function
            handler = handle

        self._receiver.receive_async(handler)

        def flush_loop():
            nonlocal last_flush
            batch_count = 0

            while not self._stop:
                time.sleep(0.5)
                now = time.monotonic()
                do_time = (now - last_flush) >= self.batch_secs

                with inflight_lock:
                    current_size = len(self._msgs)
                    do_size = current_size >= self.batch_size

                    if do_size or (do_time and self._msgs):
                        batch = self._msgs
                        self._msgs = []
                        last_flush = now
                        batch_count += 1
                    else:
                        batch = None

                if not batch:
                    continue

                try:
                    log.info("Analyzing batch of %d NOTAMs…", len(batch))

                    import asyncio
                    results = asyncio.run(
                        analyze_many(
                            batch,
                            max_concurrency=80,
                            rps=8.0,
                            timeout_sec=120.0,
                            retry_attempts=1,
                        )
                    )

                    self.repository.save_batch(results)
                    log.info(
                        "Saved batch: %d ok / %d errors",
                        sum(1 for r in results if r.get("result") is not None),
                        sum(1 for r in results if r.get("result") is None),
                    )

                except Exception as e:
                    log.exception("Batch analyze/save failed: %s", e)

        t = threading.Thread(target=flush_loop, daemon=True)
        t.start()

        try:
            while not self._stop:
                time.sleep(1.0)
                with inflight_lock:
                    n = len(self._msgs)
                if n > self.max_inflight:
                    log.warning("Backpressure: %d messages buffered (max %d)", n, self.max_inflight)
        except KeyboardInterrupt:
            self._stop = True
            t.join(timeout=5)

    def close(self):
        try:
            if self._receiver:
                self._receiver.terminate()
        finally:
            if self._svc:
                self._svc.disconnect()
            log.info("Disconnected")


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout, force=True)
    consumer = SwimConsumer()

    def _shutdown(*_):
        consumer._stop = True

    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, _shutdown)

    consumer.connect()
    try:
        consumer.run()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()