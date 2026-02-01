
# src/services/uniprot.py
import os
import json
import re
import time
import requests
import fcntl


# =====================================
# Global rate limit (cross-process): max 5 requests / 1s
# =====================================
# 说明：
# - test/map_ontology/map_all_ontology_net.py 会 fork 多进程
# - 这里用文件锁实现“全局（跨进程）”限流：1 秒窗口内最多 5 次请求
_UNIPROT_RL_RATE = int(os.getenv("UNIPROT_GLOBAL_RPS", "5"))          # 每秒最多请求数
_UNIPROT_RL_WINDOW = float(os.getenv("UNIPROT_GLOBAL_RPS_WINDOW", "1.0"))  # 窗口大小（秒）
_UNIPROT_RL_DIR = os.getenv("UNIPROT_GLOBAL_RL_DIR", "/tmp")
_UNIPROT_RL_BASENAME = "uniprot_global_ratelimit"


def _uniprot_global_rate_limit():
    """
    全局限流：保证所有进程合计在 _UNIPROT_RL_WINDOW 秒内最多 _UNIPROT_RL_RATE 次请求。
    使用 fcntl.flock 文件锁（Linux/macOS 有效；当前脚本强制 fork，通常运行在类 Unix 环境）。
    """
    if _UNIPROT_RL_RATE <= 0:
        return

    os.makedirs(_UNIPROT_RL_DIR, exist_ok=True)
    lock_path = os.path.join(_UNIPROT_RL_DIR, _UNIPROT_RL_BASENAME + ".lock")
    state_path = os.path.join(_UNIPROT_RL_DIR, _UNIPROT_RL_BASENAME + ".json")

    while True:
        # 1) lock
        with open(lock_path, "a+", encoding="utf-8") as lockf:
            fcntl.flock(lockf.fileno(), fcntl.LOCK_EX)

            now = time.monotonic()
            # 2) load state
            ts = []
            try:
                if os.path.exists(state_path):
                    with open(state_path, "r", encoding="utf-8") as sf:
                        obj = json.load(sf)
                        if isinstance(obj, list):
                            ts = [float(x) for x in obj]
            except Exception:
                ts = []

            # 3) keep within window
            window = _UNIPROT_RL_WINDOW
            rate = _UNIPROT_RL_RATE
            ts = [t for t in ts if (now - t) < window]

            if len(ts) < rate:
                ts.append(now)
                # 4) save state
                try:
                    with open(state_path, "w", encoding="utf-8") as sf:
                        json.dump(ts, sf)
                finally:
                    fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)
                return

            # need wait
            oldest = min(ts) if ts else now
            wait_s = window - (now - oldest)
            wait_s = max(float(wait_s), 0.001)

            # unlock then sleep
            fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)

        time.sleep(wait_s)


# =====================================
# UniProt search
# =====================================
def uniprot_query(query, max_sleep=30, max_retries_per_item=5, k=5):
    base_url = "https://rest.uniprot.org/uniprotkb/search"

    params = {"query": query, "format": "json", "size": k}
    sleep_time = 1

    for _ in range(max_retries_per_item):
        try:
            # 全局（跨进程）限流：1 秒最多 5 个请求（所有进程合计）
            _uniprot_global_rate_limit()

            r = requests.get(base_url, params=params, timeout=10)

            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, max_sleep)
                continue

            if r.status_code != 200:
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, max_sleep)
                continue

            data = r.json()
            results = data.get("results") or []
            return {"results": results[:k], "timeout": False}

        except Exception:
            time.sleep(sleep_time)
            sleep_time = min(sleep_time * 2, max_sleep)

    return {"results": None, "timeout": True}


# =====================================
# Extract UniProt info
# =====================================
def extract_uniprot_info(entry, rank):
    acc = entry.get("primaryAccession")

    organism = ""
    org_data = entry.get("organism", {})
    if org_data.get("scientificName"):
        organism = org_data["scientificName"]
    if org_data.get("commonName"):
        organism += f" ({org_data['commonName']})"
    if org_data.get("mnemonic"):
        organism += f" [{org_data['mnemonic']}]"

    names = []
    aliases = []

    rec = entry.get("proteinDescription", {}).get("recommendedName")
    if rec and "fullName" in rec:
        names.append(rec["fullName"]["value"])

    for alt in entry.get("proteinDescription", {}).get("alternativeNames", []):
        if "fullName" in alt:
            aliases.append(alt["fullName"]["value"])
        if "shortName" in alt:
            aliases.append(alt["shortName"]["value"])

    genes = []
    for g in entry.get("genes", []):
        if g.get("geneName", {}).get("value"):
            genes.append(g["geneName"]["value"])

    desc = []
    if names:
        desc.append("; ".join(names))
    if aliases:
        desc.append("ALIASES: " + "; ".join(sorted(set(aliases))))
    if genes:
        desc.append("GENES: " + "; ".join(sorted(set(genes))))
    if organism:
        desc.append("ORGANISM: " + organism)

    return {
        "accession": acc,
        "description": " | ".join(desc),
        "rank": rank,
    }

def search_uniprot(query, k=30, max_retries_per_item=5, max_sleep=30):
    try:
        res = uniprot_query(
            query=query,
            max_sleep=max_sleep,
            max_retries_per_item=max_retries_per_item,
            k=k,
        )
        hits_raw = res.get("results") or []
    except Exception:
        hits_raw = []
    hits = []
    for rank, entry in enumerate(hits_raw, start=1):
        info = extract_uniprot_info(entry, rank)
        hits.append(
            {
                "id": info["accession"],
                "name": info["accession"],
                "description": info["description"],
                "rank": rank,
            }
        )
    return hits

