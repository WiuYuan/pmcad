import json
import os


def process_one_folder(folder: str, input_name: str, **kwargs):
    """
    Read one JSON file and return its content as a dict.
    Used for collecting all ds.json files into a big list.
    """

    pmid = os.path.basename(folder)
    in_path = os.path.join(folder, input_name)

    # Load JSON
    try:
        with open(in_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return None, [
            {"type": "status", "name": f"load fail pmid: {pmid}"},
            {"type": "error", "msg": str(e)},
            {"type": "metric", "correct": 0, "total": 1},
        ]

    # Success — return the JSON dict
    return data, [
        {"type": "status", "name": f"ok pmid: {pmid}"},
        {"type": "metric", "correct": 1, "total": 1},
    ]
