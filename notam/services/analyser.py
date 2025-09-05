# notam/services/analyser.py
import asyncio
import logging
import random
from typing import Dict, List, Optional

from notam.analyze import analyze_notam

log = logging.getLogger(__name__)

class AsyncRateLimiter:
    """Spaces requests ~ rps if rps>0."""
    def __init__(self, rps: float):
        self.rps = float(rps)
        self.interval = 1.0 / self.rps if self.rps > 0 else 0.0
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def wait(self):
        if self.rps <= 0:
            return
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            if self._next <= now:
                self._next = now + self.interval
                return
            delay = self._next - now
            self._next += self.interval
        if delay > 0:
            await asyncio.sleep(delay)

async def analyze_many(
    items: List[Dict],
    max_concurrency: int = 80,
    *,
    rps: float = 8.0,                 # 0 = unlimited
    timeout_sec: Optional[float] = 120.0,  # per-item hard cap
    retry_attempts: int = 2,          # per item, in this pass
    retry_backoff_base: float = 1.5,  # seconds
    retry_backoff_max: float = 8.0
):
    """
    Concurrency + RPS throttle + per-item retries with exponential backoff.
    """
    sem = asyncio.Semaphore(max_concurrency)
    limiter = AsyncRateLimiter(rps) if rps and rps > 0 else None

    async def _one(item: Dict):
        # retry loop per item
        attempt = 0
        current_timeout = timeout_sec  # Initial timeout for the first attempt
        while True:
            try:
                if limiter:
                    await limiter.wait()
                async with sem:
                    coro = analyze_notam(item["icao_message"], item["issue_time"])
                    # Use adjusted timeout for retries
                    res = await (asyncio.wait_for(coro, timeout=current_timeout) if current_timeout else coro)
                if res is not None:
                    return {"input": item, "result": res, "error": None}
                # res None -> treat as transient error (connection/SDK error)
                raise RuntimeError("llm_none")
            except asyncio.TimeoutError:
                err = f"timeout>{current_timeout}s"
            except Exception as e:
                # Connection drops, DNS hiccups, etc.
                err = str(e) or "connection/error"

            # Retry decision
            if attempt >= retry_attempts:
                return {"input": item, "result": None, "error": err}

            # backoff with jitter
            sleep_s = min(retry_backoff_max, retry_backoff_base * (2 ** attempt))
            sleep_s += random.uniform(0, 0.5)
            await asyncio.sleep(sleep_s)

            # Increase timeout for subsequent retries
            current_timeout = min(current_timeout * 2,
                                  timeout_sec * 5)  # Double the timeout for each retry (up to a maximum of 5x the initial timeout)

            attempt += 1

    tasks = [_one(i) for i in items]
    return await asyncio.gather(*tasks)
