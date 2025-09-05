# # notam/services/analyser_threads.py
# import asyncio, logging, time
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from typing import Dict, List, Optional
# from notam.config import THREADS, LLM_TIMEOUT_SEC, RPS, TQDM  # <— here
# from notam.analyze import analyze_notam
#
# log = logging.getLogger(__name__)
#
# try:
#     from tqdm.auto import tqdm
# except Exception:
#     tqdm = None
#
# class _RateLimiter:
#     def __init__(self, rps: float):
#         self.rps = rps
#         import threading
#         self._tlock = threading.Lock()
#         self._next = 0.0
#         self._interval = 1.0 / rps if rps > 0 else 0.0
#
#     def wait(self):
#         if self.rps <= 0:
#             return
#         with self._tlock:
#             now = time.perf_counter()
#             if self._next <= now:
#                 self._next = now + self._interval
#                 return
#             sleep_s = self._next - now
#             self._next += self._interval
#         if sleep_s > 0:
#             time.sleep(sleep_s)
#
# def _analyze_one_sync(item: Dict, timeout_s: float, limiter: Optional[_RateLimiter]) -> Dict:
#     try:
#         if limiter:
#             limiter.wait()
#
#         async def _run():
#             return await asyncio.wait_for(
#                 analyze_notam(item["icao_message"], item["issue_time"]),
#                 timeout=timeout_s,
#             )
#         res = asyncio.run(_run())
#         return {"input": item, "result": res, "error": None}
#     except asyncio.TimeoutError:
#         return {"input": item, "result": None, "error": f"timeout>{timeout_s}s"}
#     except Exception as e:
#         log.debug("Thread error on %s: %s", item.get("notam_number"), e, exc_info=True)
#         return {"input": item, "result": None, "error": str(e)}
#
# def _run_in_threads(items: List[Dict], workers: int, timeout_s: float, rps: float, use_tqdm: bool) -> List[Dict]:
#     limiter = _RateLimiter(rps) if rps and rps > 0 else None
#     results: List[Dict] = []
#     total = len(items)
#     if total == 0:
#         return results
#
#     log.info("Analyzer: THREADS | workers=%d | timeout=%ss | rps=%s",
#              workers, timeout_s, (str(rps) if rps and rps > 0 else "unlimited"))
#
#     pbar = tqdm(total=total, desc="Analyzing NOTAMs", unit="itm", dynamic_ncols=True, mininterval=0.2) if (use_tqdm and tqdm) else None
#
#     with ThreadPoolExecutor(max_workers=workers) as ex:
#         futures = {ex.submit(_analyze_one_sync, it, timeout_s, limiter): it for it in items}
#         done = 0
#         for fut in as_completed(futures):
#             results.append(fut.result())
#             done += 1
#             if pbar:
#                 pbar.update(1)
#             else:
#                 if done % 10 == 0 or done == total:
#                     log.info("🔧 Progress: %d/%d analyzed", done, total)
#     if pbar:
#         pbar.close()
#     return results
#
# async def analyze_many(items: List[Dict], max_concurrency: int = 10):
#     # ignore max_concurrency; use hard-coded THREADS & other knobs
#     loop = asyncio.get_running_loop()
#     return await loop.run_in_executor(
#         None, lambda: _run_in_threads(items, THREADS, LLM_TIMEOUT_SEC, RPS, TQDM)
#     )
