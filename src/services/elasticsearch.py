import yaml
from pathlib import Path
import subprocess
import json
import tempfile
import requests
from typing import Optional, Dict, Any


def load_es_yaml(path):
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_es_yaml(cfg, path):
    path = Path(path)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)


def run_curl(cmd, input_json=None):
    """
    Run a curl command via subprocess.
    """
    if input_json is not None:
        p = subprocess.run(
            cmd, input=json.dumps(input_json), text=True, capture_output=True
        )
    else:
        p = subprocess.run(cmd, text=True, capture_output=True)

    if p.returncode != 0:
        raise RuntimeError(p.stderr)

    return p.stdout


def index_exists(config_path, index_name):
    cfg = load_es_yaml(config_path)
    es = cfg["elasticsearch"]

    cmd = [
        "curl",
        "-s",
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
        "--cacert",
        es["ca_cert"],
        "-u",
        f'{es["user"]}:{es["password"]}',
        f'{es["url"]}/{index_name}',
    ]

    code = run_curl(cmd).strip()
    return code == "200"


def add_index_cli(config_path, index_name, mapping=None, settings=None, description=""):
    cfg = load_es_yaml(config_path)
    if cfg.get("indices") is None:
        cfg["indices"] = {}

    if index_name in cfg["indices"]:
        raise ValueError(f"Index already registered in config: {index_name}")

    if index_exists(config_path, index_name):
        raise ValueError(f"Index already exists in ES: {index_name}")

    es = cfg["elasticsearch"]

    body = {}
    if settings:
        body["settings"] = settings
    if mapping:
        body["mappings"] = mapping

    cmd = [
        "curl",
        "-s",
        "-X",
        "PUT",
        "--cacert",
        es["ca_cert"],
        "-u",
        f'{es["user"]}:{es["password"]}',
        "-H",
        "Content-Type: application/json",
        f'{es["url"]}/{index_name}',
        "-d",
        "@-",  # ⬅⬅⬅ 必须有！！
    ]

    # 通过 stdin 发送 JSON
    p = subprocess.run(cmd, input=json.dumps(body), text=True, capture_output=True)

    if p.returncode != 0:
        print("Curl error:", p.stderr)
        raise RuntimeError("curl failed")

    print("Curl response:", p.stdout)

    cfg["indices"][index_name] = {
        "description": description,
        "mapping": mapping or {},
        "settings": settings or {},
    }
    save_es_yaml(cfg, config_path)

    print(f"✔ Index created (CLI) and registered: {index_name}")


def delete_index_cli(config_path, index_name):
    cfg = load_es_yaml(config_path)

    if not index_exists(config_path, index_name):
        raise ValueError(f"Index does not exist in ES: {index_name}")

    es = cfg["elasticsearch"]

    cmd = [
        "curl",
        "-s",
        "-X",
        "DELETE",
        "--cacert",
        es["ca_cert"],
        "-u",
        f'{es["user"]}:{es["password"]}',
        f'{es["url"]}/{index_name}',
    ]

    run_curl(cmd)

    # ---- 从 config 中删除 ----
    if "indices" in cfg and index_name in cfg["indices"]:
        del cfg["indices"][index_name]
        save_es_yaml(cfg, config_path)

    print(f"✔ Index deleted (CLI) and unregistered: {index_name}")


def bulk_insert(
    config_path: str,
    bulk_lines: list[str],
    *,
    index_name: Optional[str] = None,
    timeout: int = 120,
    raise_on_error: bool = True,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    用 requests 调 Elasticsearch _bulk（NDJSON），完全对齐你 go_index 的 bulk_index 风格。

    Parameters
    ----------
    config_path : str
        elasticsearch.yaml 路径
    bulk_lines : list[str]
        NDJSON 每一行一个 json（action 行 + source 行 + ...），不要自己加最后一行换行也行
    index_name : Optional[str]
        可选：若 action 里没写 _index，可通过 URL 指定 /{index_name}/_bulk
    timeout : int
        requests 超时
    raise_on_error : bool
        bulk 返回 errors=true 时是否抛异常
    verbose : bool
        打印失败样例

    Returns
    -------
    dict: Elasticsearch bulk response json
    """
    cfg = load_es_yaml(config_path)
    es = cfg["elasticsearch"]

    payload = "\n".join(bulk_lines)
    if not payload.endswith("\n"):
        payload += "\n"
    payload = payload.encode("utf-8")

    # /_bulk 或 /{index}/_bulk 都可以
    bulk_url = (
        f"{es['url']}/_bulk" if not index_name else f"{es['url']}/{index_name}/_bulk"
    )

    r = requests.post(
        bulk_url,
        auth=(es["user"], es["password"]),
        verify=es["ca_cert"],
        data=payload,
        headers={"Content-Type": "application/x-ndjson"},
        timeout=timeout,
    )

    # HTTP 级别错误
    if r.status_code not in (200, 201):
        if verbose:
            print("❌ Bulk HTTP error:", r.status_code)
            print(r.text[:2000])
        r.raise_for_status()

    res = r.json()

    # 关键：bulk 可能 200 但 errors=true
    if res.get("errors"):
        if verbose:
            print("❌ Bulk response has errors=true")
            # 打印前几个失败 item（避免刷屏）
            bad = []
            for it in res.get("items", []):
                # it 形如 {"index": {"status": 400, "error": {...}}}
                op = next(iter(it.keys()))
                info = it[op]
                if info.get("error"):
                    bad.append(
                        {
                            "status": info.get("status"),
                            "error": info["error"],
                            "_id": info.get("_id"),
                        }
                    )
                if len(bad) >= 5:
                    break
            print(json.dumps(bad, indent=2)[:2000])

        if raise_on_error:
            raise RuntimeError(
                "Bulk indexing completed with errors=true (see printed samples)."
            )

    return res


def search_via_curl(config_path, index_name, query_json):
    cfg = load_es_yaml(config_path)
    es = cfg["elasticsearch"]
    es_url = es["url"]

    r_knn = requests.post(
        f"{es_url}/{index_name}/_search",
        auth=(es["user"], es["password"]),
        verify=es["ca_cert"],
        json=query_json,
    )

    return r_knn.json()["hits"]["hits"]
