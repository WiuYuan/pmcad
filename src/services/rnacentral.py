
# pmcad/src/services/rnacentral.py
import os
import json
import time
import requests
import fcntl


# =====================================
# Global rate limit (cross-process): max 5 requests / 1s
# =====================================
# 说明：
# - map_all_ontology_net.py 会 fork 多进程
# - 这里用文件锁实现“全局（跨进程）”限流：1 秒窗口内最多 N 次请求（默认 5）
_RNACENTRAL_RL_RATE = int(os.getenv("RNACENTRAL_GLOBAL_RPS", "5"))
_RNACENTRAL_RL_WINDOW = float(os.getenv("RNACENTRAL_GLOBAL_RPS_WINDOW", "1.0"))
_RNACENTRAL_RL_DIR = os.getenv("RNACENTRAL_GLOBAL_RL_DIR", "/tmp")
_RNACENTRAL_RL_BASENAME = "rnacentral_global_ratelimit"


def _rnacentral_global_rate_limit():
    if _RNACENTRAL_RL_RATE <= 0:
        return

    os.makedirs(_RNACENTRAL_RL_DIR, exist_ok=True)
    lock_path = os.path.join(_RNACENTRAL_RL_DIR, _RNACENTRAL_RL_BASENAME + ".lock")
    state_path = os.path.join(_RNACENTRAL_RL_DIR, _RNACENTRAL_RL_BASENAME + ".json")

    while True:
        with open(lock_path, "a+", encoding="utf-8") as lockf:
            fcntl.flock(lockf.fileno(), fcntl.LOCK_EX)

            now = time.monotonic()
            ts = []
            try:
                if os.path.exists(state_path):
                    with open(state_path, "r", encoding="utf-8") as sf:
                        obj = json.load(sf)
                        if isinstance(obj, list):
                            ts = [float(x) for x in obj]
            except Exception:
                ts = []

            window = _RNACENTRAL_RL_WINDOW
            rate = _RNACENTRAL_RL_RATE
            ts = [t for t in ts if (now - t) < window]

            if len(ts) < rate:
                ts.append(now)
                try:
                    with open(state_path, "w", encoding="utf-8") as sf:
                        json.dump(ts, sf)
                finally:
                    fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)
                return

            oldest = min(ts) if ts else now
            wait_s = window - (now - oldest)
            wait_s = max(float(wait_s), 0.001)

            fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)

        time.sleep(wait_s)


# =====================================
# RNAcentral search (EBI Search API)
# =====================================
def rnacentral_query(
    term,
    organism=None,
    max_sleep=30,
    max_retries_per_item=5,
    top_k=5,
):
    base_url = "https://www.ebi.ac.uk/ebisearch/ws/rest/rnacentral"

    q = str(term).strip()
    if organism:
        q = f'{q} "{organism}"'

    params = {
        "query": q,
        "size": top_k,
        "format": "json",
        # 🔑 explicitly request annotations
        "fields": "description,species,gene,rna_type",
    }

    sleep_time = 1
    for _ in range(max_retries_per_item):
        try:
            # 全局（跨进程）限流：1 秒最多 5 个请求（所有进程合计）
            _rnacentral_global_rate_limit()

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
            return {"results": data.get("entries", []), "timeout": False}

        except Exception:
            time.sleep(sleep_time)
            sleep_time = min(sleep_time * 2, max_sleep)

    return {"results": None, "timeout": True}


# =====================================
# Build unified RNAcentral description
# =====================================
def build_rnacentral_description(entry):
    fields = entry.get("fields", {})

    parts = []

    if fields.get("description"):
        parts.append(" | ".join(fields["description"]))

    if fields.get("gene"):
        parts.append("GENE: " + ", ".join(fields["gene"]))

    if fields.get("rna_type"):
        parts.append("TYPE: " + ", ".join(fields["rna_type"]))

    if fields.get("species"):
        parts.append("SPECIES: " + ", ".join(fields["species"]))

    return " | ".join(parts)

# =====================================
# Extract RNAcentral info
# =====================================
def extract_rnacentral_info(entry, rank: int):
    rid = entry.get("id")  # RNAcentral accession
    desc = build_rnacentral_description(entry)

    return {
        "id": rid,
        "name": rid,                # 和你现在的写法一致：name=accession
        "description": desc or "",
        "rank": rank,
    }


# =====================================
# RNAcentral search (UniProt-aligned)
# =====================================
def search_rnacentral(
    term: str,
    organism: str = None,
    k: int = 30,
    max_retries_per_item: int = 5,
    max_sleep: int = 30,
):
    """
    Return hits list:
      [{"id":..., "name":..., "description":..., "rank":...}, ...]
    """
    try:
        res = rnacentral_query(
            term=term,
            organism=organism,
            max_sleep=max_sleep,
            max_retries_per_item=max_retries_per_item,
            top_k=k,
        )
        hits_raw = res.get("results") or []
    except Exception:
        hits_raw = []

    hits = []
    for rank, entry in enumerate(hits_raw[:k], start=1):
        info = extract_rnacentral_info(entry, rank)
        # 跳过没有 id 的脏数据（可选）
        if not info.get("id"):
            continue
        hits.append(info)

    return hits

