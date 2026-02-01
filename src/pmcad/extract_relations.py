import os
import json
from tqdm import tqdm
import concurrent.futures
import csv
import pandas as pd
from nltk.tokenize import sent_tokenize
import nltk
from src.services.llm import LLM
from src.pmcad.pmidstore import PMIDStore

# nltk.download("punkt", quiet=True)
# nltk.download("punkt_tab", quiet=True)

def build_interleaved_background(history):
    """
    history: list of dict
      [
        {
          "sentence": "...",
          "relations": [...]
        },
        ...
      ]
    """
    blocks = []

    for item in history:
        sent = item["sentence"]
        rels = item["relations"]

        blocks.append(sent)


    return " ".join(blocks)

def build_prompt(background: str, current_sentence: str) -> str:
    """
    Build prompt for unified relation extraction with strict naming rules,
    8 controlled relations, and evidence replaced by short justification.
    """

    prompt = """

You are a biomedical relation extraction system.
Your task is to extract all relations explicitly present in current sentence, representing them in the structured schema below.

Each extracted relation is composed of entities (components, targets, contexts and corresponding meta field) and a relation:
- components: the source entities that perform or initiate the relation
- targets: the entities that receive or are affected by the relation
- contexts: the biological or experimental background within which the relation occurs (e.g., species, cell type, anatomical structure, disease, experimental condition)

Every entity must include:
- name: the canonical entity name used to represent the concept in the schema, MUST be not equal to type.
- type: one of the allowed ontology-aligned categories
- description: a concise semantic explanation (required for GO, SO (sequence ontology), domain, anatomy, cell_type, disease, chemical; For gene, protein, species, and RNA entities, description MUST NOT be included.)
- meta: optional list of additional entity attributes (each with name, type, description)
- For gene and protein components/targets, meta SHOULD include species whenever the sentence makes it known.

A relation connects components → targets and is embedded in a contexts.
The relation must contain:
- name: RO-style label (no numeric IDs)
- description: a brief semantic explanation of the relation's meaning

============================================================
OUTPUT STRUCTURE

[
  {{
    "components": [
      {{"name": "...", "description": "...", "type": "...", "meta": [{{"name": "...", "description": "...", "type": "..."}}, ...]}},
      ...
    ],
    "relation": {{"name": "", "description": "..."}},
    "targets": [
      {{"name": "...", "description": "...", "type": "...", "meta": [{{"name": "...", "description": "...", "type": "..."}}, ...]}},
      ...
    ],
    "contexts": [
      {{"name": "...", "description": "...", "type": "...", "meta": [{{"name": "...", "description": "...", "type": "..."}}, ...]}},
      ...
    ],
  }},
  ...
]

============================================================
ALLOWED ENTITY TYPES
- "gene"
- "protein"
- "RNA"
- "GO"
- "chemical"
- "cell_type"
- "cell_line"
- "anatomy"
- "disease"
- "SO"
- "species"
- "domain"     (protein domains, binding sites, active sites, etc.)

============================================================
ENTITY NAMING RULES:

gene:
  - Exact **ONE** gene symbol or ORF label (“TP53”, “J4R”, “ORF1a”).
  - MUST include species type in meta.

protein:
  - Exact **ONE** protein name (“p53 protein”, “spike glycoprotein”).
  - OR a single, well-defined protein or protein domain defined by a specific biochemical function or structural role explicitly stated in the text.
  - No accessions.
  - MUST include species type in meta.

domain:
  - A structurally or functionally defined region of a protein explicitly described in the text,
    including but not limited to:
    binding site,
    binding pocket,
    active site,
    catalytic site,
    enzymatic domain,
    interaction site,
    interaction surface,
    substrate-binding site,
    ligand-binding site,
    N-terminal domain,
    C-terminal domain.

RNA:
  - Exact **ONE** RNA entity.
  - MUST refer to a **specific, biologically instantiated RNA molecule tied to a concrete organism, virus, or genomic source** 
    (e.g., “human TP53 mRNA”).
  - Generic or class-level RNA terms that do NOT specify an exact biological instance
    (e.g., “mRNA”, “tRNA”, “rRNA”, “viral RNA”, “host mRNA”) 
    MUST NOT be annotated as RNA.
  - MUST include species type in meta.

GO:
  - Any phrase describing a molecular function, biological process or cellular component.
  - MUST NOT include numeric GO identifiers (e.g., GO:0003677 forbidden).
  - MUST with a description
  - MUST be not specific to any particular organism
  - MUST NOT include any specific species in name and description
  
chemical:
  - Exact chemical name.
  - MUST refer to a specific, concrete chemical entity
  - MUST provide a brief chemical description.

species:
  - Exact species or taxonomic name.
  - The term may correspond to any biological taxonomic rank.
  — MUST NOT include description field.

SO:
  - MUST correspond to an explicit Sequence Ontology (SO) term present in the text,
    describing a sequence feature on DNA, RNA, or protein.
  - Examples include:
      “5' cap”, “poly(A) site”, “splice acceptor site”, 
      “promoter”, “enhancer”, “exon”, “intron”, “UTR”, “open reading frame”,
      “TATA box”, “CpG island”.
  - The mention MUST describe a real sequence feature, not a general concept.
  - MUST provide a brief description copied or summarized from the ontology (SO).
  - MUST NOT include numeric accessions (SO:0000204 forbidden in extracted text).
  - If multiple SO terms appear, extract them individually as separate features.
  - MUST be not specific to any particular organism

disease:
  - name MUST be a specific, named disease entity.
  - Examples:
      “Alzheimer's disease”, “hepatocellular carcinoma”, “COVID-19”, 
      “acute myeloid leukemia”, “breast cancer”.
  - MUST NOT use DOID identifiers (DOID:1234 forbidden).
  - MUST provide a disease description (from DOID ontology).
  - The pathogen itself (such as a virus or bacterium) is not the disease. The disease is the result caused by the pathogen.
  
anatomy:
  - MUST correspond to an anatomical structure (multi-cellular or organ-level)
    explicitly mentioned in the text.
  - Examples:
      “lung”, “heart”, “nasal epithelium”, “lymph node”, 
      “skeletal muscle”, “blood vessel”, “intestine”.
  - MUST provide a UBERON-derived description.
  - MUST NOT include numerical ontology IDs (UBERON:nnnn forbidden in text).
  - MUST NOT include cell types (handled by cell_type).
  - MUST NOT include subcellular organelles (handled by location).
  
cell_type:
  - MUST correspond to a specific CL term.
  - name MUST NOT contain species words or adjectives.
    If the text says "rat stem cell", extract:
      cell_type: "stem cell"
      contexts: species "Rattus norvegicus"
  - MUST provide a CL-based description.
    
cell_line:
  - MUST correspond to a specific named cell line.
    (e.g., "HeLa", "HEK293", "Jurkat", "CHO cells").
  - name MUST be exactly the cell line name without species words.
  - MUST provide a cell-line description (from Cellosaurus/CLO-style definition).
  - Species MUST NOT appear in the name.
  - SHOULD include species as a meta entry when known or implied
    (e.g., HeLa -> Homo sapiens).

============================================================
RELATION RULES:

You may use relations taken from the Relation Ontology (RO).
Each relation must include a name and a brief description, but must not include any RO identifier (e.g., “RO:0002213” is not allowed).

Use only the RO labels and their semantic meanings when specifying a relation.

============================================================
CONTEXTS RULES:

1. If a relation occurs in a specific biological background explicitly mentioned 
   in the CURRENT SENTENCE — such as a species, anatomical structure, 
   cell type or disease — you MUST include these 
   as separate contexts entities in the "contexts" list.

2. Species, anatomy, cell_type, disease, and GO entities MAY appear in the 
   "contexts" list when they describe the biological setting of the relation.

3. When an entity is placed in the "contexts" list, ONLY add "meta" fields 
   to gene, protein, or RNA entities inside the contexts.

============================================================
META RULES:

Each meta entry must itself be a valid entity and follow all rules of its entity type.

============================================================
OUTPUT RULES:

1. Output ONLY a JSON list.
2. Each element MUST follow the defined schema.
3. If no relations exist, output [].
4. Use the information from the contexts to extract relations from current sentence.
5. Keep the "components" list as small as possible:
    - Include only the minimal set of entities necessary to express the relation.
    - Do NOT add extra entities that are not required for the relation.
    - Avoid grouping entities unless they jointly act as a single source.
6. The relation can be infered from the sentence.
"""

    article = f"""
============================================================
BACKGROUND (relations before the current one):

{background + ' ' + current_sentence}
============================================================
CURRENT SENTENCE:

{current_sentence}"""
    return prompt + article
  
def extract_json_array(raw: str) -> str:
    """
    从 LLM 输出中尽量提取出 JSON array 字符串：
    - 正常情况：匹配到 [...] 直接返回
    - 兼容情况：只输出了单个对象 {...} 时，自动包装成 [{...}]
    - 允许 raw 前后有额外文本/代码块/解释
    """
    decoder = json.JSONDecoder()

    # 优先用 JSONDecoder 做“智能截取”（能处理前后有杂质文本）
    for i, ch in enumerate(raw):
        if ch not in "[{":
            continue
        try:
            obj, end = decoder.raw_decode(raw[i:])
        except json.JSONDecodeError:
            continue

        snippet = raw[i : i + end]
        if isinstance(obj, list):
            return snippet
        if isinstance(obj, dict):
            return "[" + snippet + "]"

    # 兜底：保持原逻辑（但放最后）
    start = raw.find("[")
    end = raw.rfind("]")
    if start != -1 and end != -1 and end >= start:
        return raw[start : end + 1]

    raise ValueError("No valid JSON array/object found")

def process_one_folder_llm_get_relations(
    pmid: int, store: PMIDStore, output_file_name: str, llm: LLM
):
    """
    process_folder_parallel 专用的 process_one_folder：
    - folder 形如: /root/.../<pmid>
    - 自动读取 abstract.txt
    - 调用 LLM → 得到关系 → 写 output_file_name
    - 返回 (result, info_list)
    """
    pmid = int(pmid)
    abstract = store.get_abstract(pmid)

    # -----------------------------
    # 调用 LLM
    # -----------------------------
    n_total = 0
    n_correct = 0
    n_error = 0
    history = []
    all_relations = []
    sentences = sent_tokenize(abstract)
    for i, sent in enumerate(sentences):
        n_total += 1
        background = build_interleaved_background(history)
        prompt = build_prompt(background, sent)
        try:
            raw = llm.query(prompt)
        except Exception as e:
            n_error += 1
            continue

        try:
            rels = json.loads(extract_json_array(raw))
        except:
            rels = json.loads(extract_json_array(raw))
            print("\n"+raw+"\n")
            continue
          
        n_correct += 1

        all_relations.append({"sentence": sent, "rel_from_this_sent": rels})
        history.append({
            "sentence": sent,
            "relations": rels
        })

    data = {
        "pmid": pmid,
        "abstract": abstract,
        "relations": all_relations,
        "error": None,
    }

    # -----------------------------
    # 写 ds.json
    # -----------------------------
    store.put(pmid, output_file_name, data)

    return None, [
        {"type": "status", "name": "success", "description": f"{pmid}"},
        {"type": "metric", "name": "judge", "correct": n_correct, "total": n_total},
        {"type": "metric", "name": "llm_error", "correct": n_error, "total": n_total},
    ]


def delete_all_file(folder, filename):
    """
    删除 folder 下所有 PMIDs 目录中的 ds.json
    结构：folder/<pmid>/filename
    """
    pmids = [d for d in os.listdir(folder) if os.path.isdir(os.path.join(folder, d))]

    count = 0

    for pmid in tqdm(pmids, desc=f"Deleting {filename}"):
        ds_path = os.path.join(folder, pmid, filename)
        if os.path.exists(ds_path):
            os.remove(ds_path)
            count += 1

    print(f"\nDeleted {count} {filename} files.")