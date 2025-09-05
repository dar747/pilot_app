# notam/config.py
# --- Hard-coded runtime knobs (edit these as you like) ---
USE_THREADS       = True   # use the threaded analyzer
THREADS           = 64     # how many worker threads to run
LLM_TIMEOUT_SEC   = 120.0   # per-item hard timeout for the LLM call
RPS               = 0.0    # 0 = unlimited; set e.g. 8.0 if you hit 429s
TQDM              = True   # show progress bar if tqdm is installed
VERBOSE_ANALYSIS  = False  # print full JSON analyses (keep False for speed)
