from src.pmcad.parallel_process import process_folder_parallel
from src.services.pubmed import process_one_folder_download_abstract, process_one_folder_judge_abstract



folder = "/data/wyuan/workspace/pmcdata_pro/data/pattern/rna_capping"
results = process_folder_parallel(
    folder=folder,
    process_one_folder=process_one_folder_judge_abstract,
    input_name="abstract.tsv",
    workers=32,
)