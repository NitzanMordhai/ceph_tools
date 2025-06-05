import os
import re
import json
import yaml
import fnmatch
import datetime
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator, Optional

# -----------------------------------------------------------------------------
# CONFIGURABLE CONSTANTS                                                         
# -----------------------------------------------------------------------------
ROOT_DIR = "/a"
FILE_PATTERN = "json_output.*.smithi*.front.sepia.ceph.com"
MAX_WORKERS = 32
LIMIT = None  # None -> process everything
# ---- PostgREST endpoint ------------------------------------------------------
#   • Override with environment variable POSTGREST_URL if needed
#   • Default now points to mira118 server (matches your deployment)
POSTGREST_URL = os.getenv(
    "POSTGREST_URL",
    "http://mira118.front.sepia.ceph.com:4000/cbt_performance"
)
AUTH = (
    os.getenv("POSTGREST_USER", "postgres"),
    os.getenv("POSTGREST_PASS", "root")
)

# -----------------------------------------------------------------------------
# DISCOVERY – fast iterator (stops when LIMIT is reached)                        
# -----------------------------------------------------------------------------

def iter_matching_files(root_dir: str, pattern: str, limit: Optional[int] = None) -> Generator[str, None, None]:
    """Yield file paths whose *filename* matches the given pattern.
    Traverses the tree with os.walk; stops once *limit* results have been found."""
    compiled = re.compile(fnmatch.translate(pattern))
    found = 0
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if compiled.match(filename):
                yield os.path.join(dirpath, filename)
                found += 1
                if limit and found >= limit:
                    return

# -----------------------------------------------------------------------------
# YAML / JSON HELPERS                                                            
# -----------------------------------------------------------------------------

def get_teuthology_config(start_path: str) -> Optional[dict]:
    path = Path(start_path)
    for parent in path.parents:
        cfg = parent / "orig.config.yaml"
        if cfg.exists():
            try:
                return yaml.safe_load(cfg.read_text())
            except Exception as exc:
                print(f"⚠️  Failed reading {cfg}: {exc}")
                return None
    return None


def load_json(path: str) -> Optional[dict]:
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as exc:
        print(f"⚠️  Invalid JSON {path}: {exc}")
        return None

# -----------------------------------------------------------------------------
# BENCHMARK HELPERS                                                              
# -----------------------------------------------------------------------------

def extract_timestamp(job_name: str) -> Optional[datetime.datetime]:
    m = re.search(r"\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}", job_name)
    return datetime.datetime.strptime(m[0], "%Y-%m-%d_%H:%M:%S") if m else None


def read_total_cpu_cycles(testdir: str) -> int:
    total = 0
    for p in Path(testdir).rglob('perf_stat.*'):
        try:
            match = re.search(r"(.*) cycles", p.read_text())
            if match:
                total += int(match.group(1).replace(',', '').strip())
        except Exception:
            continue
    return total

# -----------------------------------------------------------------------------
# PAYLOAD BUILD + POSTGREST                                                      
# -----------------------------------------------------------------------------

def build_payload(cfg: dict, bench_json: dict, path: str) -> Optional[dict]:
    try:
        ts = extract_timestamp(cfg.get("name", ""))
        if not ts:
            return None

        cbt_task = next((d.get("cbt") for d in cfg.get("tasks", []) if "cbt" in d), None)
        if not cbt_task:
            return None

        path_obj = Path(path)
        seq_match = re.search(r"/json_output\.(\d+)", path)
        benchmark_mode = path_obj.parent.name if path_obj.parent.name in {"rand", "write", "seq"} else "fio"

        return {
            "job_id": cfg["job_id"],
            "started_at": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "branch": cfg.get("branch"),
            "sha1": cfg.get("sha1"),
            "os_type": cfg.get("os_type"),
            "os_version": cfg.get("os_version"),
            "machine_type": cfg.get("machine_type"),
            "benchmark_mode": benchmark_mode,
            "seq": int(seq_match.group(1)) if seq_match else 0,
            "total_cpu_cycles": read_total_cpu_cycles(os.path.dirname(path)),
            "benchmark": cbt_task.get("benchmarks"),
            "results": bench_json.get("results")
        }
    except Exception as exc:
        print(f"⚠️  Payload error {path}: {exc}")
        return None


def send_payload(payload: dict):
    try:
        r = requests.post(POSTGREST_URL, json=payload, auth=AUTH, timeout=10)
        if r.status_code != 201:
            print(f"❌ Insert failed ({r.status_code}): {r.text} -> {payload['job_id']}")
    except requests.exceptions.RequestException as exc:
        print(f"❌ HTTP error: {exc}")

# -----------------------------------------------------------------------------
# PROCESS SINGLE FILE                                                            
# -----------------------------------------------------------------------------

def process_file(path: str):
    cfg = get_teuthology_config(path)
    if not cfg:
        return

    bench_json = load_json(path)
    if not bench_json:
        return

    payload = build_payload(cfg, bench_json, path)
    if payload:
        send_payload(payload)

# -----------------------------------------------------------------------------
# MAIN                                                                           
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"🔍 Scanning {ROOT_DIR} for pattern '{FILE_PATTERN}' (limit={LIMIT}) …")
    print(f"📡 Using PostgREST endpoint: {POSTGREST_URL}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = []
        for file_path in iter_matching_files(ROOT_DIR, FILE_PATTERN, LIMIT):
            futures.append(pool.submit(process_file, file_path))

        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                print(f"Unhandled error: {exc}")

    print("✅ Done.")
