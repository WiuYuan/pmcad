import os
import json
from tqdm import tqdm
import concurrent.futures
import csv
import pandas as pd
from nltk.tokenize import sent_tokenize
import nltk
from src.services.llm import LLM

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

        if rels:
            blocks.append(
                json.dumps(rels, ensure_ascii=False, indent=2)
            )

    return "\n".join(blocks)

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
- description: a concise semantic explanation (required for GO, SO (sequence ontology), domain, anatomy, cell_type, disease; For gene, protein, species, chemical, and RNA entities: description MUST NOT be included.)
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

species:
  - Exact species or taxonomic name.
  - The term may correspond to any biological taxonomic rank,
    including but not limited to:
      species, subspecies, genus, family, or higher viral taxonomic groups.
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
  - MUST correspond to a specific CL term explicitly mentioned in the text.
  - Examples:
      “macrophage”, “dendritic cell”, “T cell”, “B cell”, 
      “epithelial cell”, “neuron”, “astrocyte”, “microglia”.
  - MUST provide a CL-based description.
  - Cell states (“activated T cell”, “immature dendritic cell”) are allowed
    ONLY if directly appearing in the text.
  - SHOULD NOT include species name inside the cell-type token
    (e.g., "human macrophage" → cell_type: “macrophage”, species: “Homo sapiens”).

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

{background}
============================================================
CURRENT SENTENCE:

{current_sentence}"""
    return prompt + article

def extract_json_array(raw: str):
    start = raw.find("[")
    end = raw.rfind("]")
    if start == -1 or end == -1 or end < start:
        raise ValueError("No valid JSON array found")
    return raw[start:end + 1]

def process_one_folder_llm_get_relations(
    folder: str, output_file_name: str, llm: LLM, require_file: str | None = None
):
    """
    process_folder_parallel 专用的 process_one_folder：
    - folder 形如: /root/.../<pmid>
    - 自动读取 abstract.txt
    - 调用 LLM → 得到关系 → 写 output_file_name
    - 返回 (result, info_list)
    """

    pmid = os.path.basename(folder)
    tsv_path = os.path.join(folder, "abstract.tsv")
    out_path = os.path.join(folder, output_file_name)

    if require_file is not None:
        # 找到以 require_file 开头的所有文件
        candidates = [
            fname
            for fname in os.listdir(folder)
            if fname.startswith(require_file) and fname.endswith(".tsv")
        ]

        if not candidates:
            # 没有任何匹配文件，直接跳过（不算一个样本）
            return None, [
                {
                    "type": "status",
                    "name": f"skip pmid:{pmid} (no file startswith '{require_file}')",
                },
                {"type": "metric", "correct": 0, "total": 0},
            ]

        # 这里简单地取第一个匹配的文件，如果你有多种版本命名规则，可以在这里再排个序
        dep_path = os.path.join(folder, candidates[0])

        try:
            df = pd.read_csv(dep_path, sep="\t")

            # 示例：你的文件头类似
            # Unnamed: 0.1  Unnamed: 0  pmid  gene  species  function  GO_ID  GO_name  gene_ids
            # “读取之后什么都没有” → df 只有表头、没有数据行
            if df.empty or len(df) == 0:
                return None, [
                    {
                        "type": "status",
                        "name": f"skip pmid:{pmid} (empty TSV: {candidates[0]})",
                    },
                    {"type": "metric", "correct": 0, "total": 0},
                ]

        except Exception as e:
            # 依赖 TSV 读失败，也选择跳过（当作“条件不满足”，而不是 hard error）
            return None, [
                {
                    "type": "status",
                    "name": f"skip pmid:{pmid} (fail to read {candidates[0]})",
                },
                {"type": "error", "msg": str(e)},
                {"type": "metric", "correct": 0, "total": 0},
            ]
    # -----------------------------
    # 读取 abstract.tsv
    # -----------------------------
    if not os.path.exists(tsv_path):
        data = {
            "pmid": pmid,
            "abstract": None,
            "relations": None,
            "error": "abstract.tsv not found",
        }
        try:
            with open(out_path, "w", encoding="utf-8") as fw:
                json.dump(data, fw, ensure_ascii=False, indent=2)
            return data, [
                {"type": "status", "name": f"missing abstract.tsv pmid:{pmid}"},
                {"type": "metric", "correct": 0, "total": 1},
            ]
        except Exception as e:
            return None, [
                {"type": "status", "name": f"write fail pmid:{pmid}"},
                {"type": "error", "msg": str(e)},
                {"type": "metric", "correct": 0, "total": 1},
            ]

    # -----------------------------
    # 从 TSV 查找对应 PMID 的 abstract
    # -----------------------------
    abstract = None
    try:
        with open(tsv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                if row.get("pmid") == pmid:
                    abstract = row.get("content", "").strip()
                    break
    except Exception as e:
        return None, [
            {"type": "status", "name": f"read tsv fail pmid:{pmid}"},
            {"type": "error", "msg": str(e)},
            {"type": "metric", "correct": 0, "total": 1},
        ]

    # -----------------------------
    # abstract 缺失或为空
    # -----------------------------
    if not abstract:
        data = {
            "pmid": pmid,
            "abstract": abstract,
            "relations": None,
            "error": "Empty or missing abstract in TSV",
        }
        try:
            with open(out_path, "w", encoding="utf-8") as fw:
                json.dump(data, fw, ensure_ascii=False, indent=2)
            return data, [
                {"type": "status", "name": f"empty abstract pmid:{pmid}"},
                {"type": "metric", "correct": 0, "total": 1},
            ]
        except Exception as e:
            return None, [
                {"type": "status", "name": f"write fail pmid:{pmid}"},
                {"type": "error", "msg": str(e)},
                {"type": "metric", "correct": 0, "total": 1},
            ]

    # -----------------------------
    # 调用 LLM
    # -----------------------------
    history = []
    try:
        all_relations = []
        sentences = sent_tokenize(abstract)
        for i, sent in enumerate(sentences):
            background = build_interleaved_background(history)
            prompt = build_prompt(background, sent)

            raw = llm.query(prompt)

            try:
                rels = json.loads(extract_json_array(raw))
            except:
                continue

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

        ok = True

    except Exception as e:
        data = {
            "pmid": pmid,
            "abstract": abstract,
            "relations": None,
            "error": str(e),
        }
        ok = False

    # -----------------------------
    # 写 ds.json
    # -----------------------------
    try:
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(data, fw, ensure_ascii=False, indent=2)

        return data, [
            {"type": "status", "name": f"processed pmid:{pmid}"},
            {"type": "metric", "correct": 1 if ok else 0, "total": 1},
        ]

    except Exception as e:
        return None, [
            {"type": "status", "name": f"write fail pmid:{pmid}"},
            {"type": "error", "msg": str(e)},
            {"type": "metric", "correct": 0, "total": 1},
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
