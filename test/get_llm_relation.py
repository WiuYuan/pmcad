from src.services.llm import LLM
from src.pmcad.parallel_process import process_folder_parallel
from src.pmcad.extract_relations import process_one_folder_llm_get_relations
from src.pmcad.pmidstore import PMIDStore

llm = LLM(
    model_name="qwen3-8b",
    llm_url="http://11.54.1.1:18000/v1/completions",
    format="openai_completion",
)

# pmidlist = ["27427769", "18495773", "18305027", "17267492", "20862256", "22022449", "19850911", "26098995", "18571739", "16912287"]
pmidlist = ["14716684"]
pmidlist = None
pmid_file = "/lustre1/lqian/wyuan/pmid/pmids_rcr_ge_23.62.txt"

with open(pmid_file, "r") as f:
    pmidlist = [line.strip() for line in f if line.strip()]

print(len(pmidlist))

store = PMIDStore("/lustre1/lqian/wyuan/pmid.db")

print(llm.query("who are you"))

raise

process_folder_parallel(
    store=store,
    process_one_folder=process_one_folder_llm_get_relations,
    workers=10,
    output_file_name="qw",
    llm=llm,
    pmidlist=pmidlist,
    # limit=1,
    # require_file="final_file_old"
)