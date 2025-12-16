import os
import json
import time
import requests
import xml.etree.ElementTree as ET

def clean_xml_text(s: str) -> str:
    # 移除 PubMed 中偶尔出现的非法控制字符
    return "".join(ch for ch in s if ch.isprintable() or ch in "\n\r\t")

def fetch_abstract_ncbi_forever(pmid: str, rate_limit_obj: dict):
    """
    无限 retry 下载 abstract。
    返回字符串："NO_ABSTRACT" / "NO_ARTICLE" / 实际 abstract 文本。
    """

    RATE_LIMIT = rate_limit_obj.get("rate", 1)  # 默认 1 req/s

    def rate_limit():
        last = rate_limit_obj["t"]
        now = time.time()
        wait = 1.0 / RATE_LIMIT
        if now - last < wait:
            time.sleep(wait - (now - last))
        rate_limit_obj["t"] = time.time()

    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

    while True:
        try:
            rate_limit()
            r = requests.get(url, params={
                "db": "pubmed",
                "id": pmid,
                "retmode": "xml"
            }, timeout=10)
            r.raise_for_status()

            root = ET.fromstring(r.text)
            article = root.find(".//PubmedArticle")
            if article is None:
                return "NO_ARTICLE"

            abstract_node = article.find(".//Abstract")
            if abstract_node is None:
                return "NO_ABSTRACT"

            nodes = abstract_node.findall("AbstractText")
            if not nodes:
                return "NO_ABSTRACT"

            parts = []
            for node in nodes:
                txt = "".join(node.itertext()).strip()
                txt = clean_xml_text(txt)
                if txt:
                    label = node.attrib.get("Label")
                    parts.append(f"{label}: {txt}" if label else txt)

            return "\n\n".join(parts) if parts else "NO_ABSTRACT"

        except Exception as e:
            time.sleep(1)
            continue
        
        
def process_one_folder_download_abstract(
    folder: str,
    output_name: str = "abstract.tsv",
    rate_limit_obj={"rate": 1, "t": 0.0},
):
    """
    兼容 process_folder_parallel 的目录处理函数：
    - folder: /path/to/.../<pmid>
    - 下载 abstract → 保存为 TSV:   pmid\tcontent
    """

    pmid = os.path.basename(folder)
    out_path = os.path.join(folder, output_name)

    # 下载
    abstract = fetch_abstract_ncbi_forever(pmid, rate_limit_obj)
    abstract = abstract.replace("\n", " ").replace("\r", " ").strip()

    # 写 TSV
    try:
        with open(out_path, "w", encoding="utf-8") as fw:
            fw.write("pmid\tcontent\n")
            fw.write(f"{pmid}\t{abstract}\n")
    except Exception as e:
        return None, [
            {"type": "status", "name": f"write fail pmid:{pmid}"},
            {"type": "error", "msg": str(e)}
        ]

    # 统计：abstract 是否成功（不是缺失）
    success = abstract not in ("NO_ABSTRACT", "NO_ARTICLE")

    return abstract, [
        {"type": "status", "name": f"pmid:{pmid}"},
        {
            "type": "metric",
            "name": "abstract",
            "correct": 1 if success else 0,
            "total": 1
        }
    ]
    
    
def process_one_folder_judge_abstract(
    folder: str,
    input_name: str = "abstract.tsv",
    min_length: int = 50,
    end_char_list: list = ['.']
):
    """
    读取 folder 下的 abstract.tsv，判断：
      - abstract 是否 < 50 字符（排除 NO_ABSTRACT / NO_ARTICLE）
      - abstract 是否不以 '.' 结尾
    不保存任何文件，只返回给 parallel_process 用的 result & info_list。
    """

    pmid = os.path.basename(folder)
    in_path = os.path.join(folder, input_name)

    if not os.path.exists(in_path):
        return None, [
            {"type": "status", "name": f"skip pmid {pmid} (no abstract.tsv)"},
            {"type": "metric", "name": "judge_abstract", "correct": 0, "total": 1}
        ]

    # --- Load abstract.tsv ---
    try:
        with open(in_path, "r", encoding="utf-8") as f:
            lines = f.read().split("\n")

        if len(lines) < 2:
            abstract = ""
        else:
            # 第二行格式：pmid \t content
            parts = lines[1].split("\t", 1)
            abstract = parts[1] if len(parts) > 1 else ""

    except Exception as e:
        return None, [
            {"type": "status", "name": f"load fail pmid {pmid}"},
            {"type": "error", "msg": str(e)},
        ]

    too_short = False
    no_final_period = False

    # 仅检测真正的 abstract
    if abstract not in ("NO_ABSTRACT", "NO_ARTICLE", ""):

        if len(abstract) < min_length:
            too_short = True
            
        stripped = abstract.strip()

        if not any(stripped.endswith(ch) for ch in end_char_list):
            no_final_period = True

    # 返回结果（不保存）
    result = {
        "pmid": pmid,
        "abstract": abstract,
        "length": len(abstract),
        "too_short": too_short,
        "no_final_period": no_final_period
    }

    # metric：完整的定义是两个条件都正常
    complete = (not too_short) and (not no_final_period)
    correct = 1 if complete else 0

    return result, [
        {"type": "status", "name": f"pmid:{pmid}"},
        {"type": "metric", "name": "judge_abstract", "correct": correct, "total": 1},
    ]