def search_cpr(
    query=None,
    k=10,
    verbose=True,
):
    """
    Pseudo search for ChemProt CPR classes.
    Keeps the same return format as search_ontology():
      [
        {"id","name","description","dense_rank","splade_rank","rank"},
        ...
      ]

    Notes:
    - No retrieval/reranking; ranks are fixed in CPR order.
    - k controls how many classes you want returned (default 10 => CPR:1..CPR:10)
    - query is accepted for API symmetry but unused.
    """

    # CPR class inventory (ChemProt). CPR:10 is "NOT" (no relation).
    cpr_defs = [
        ("CPR:1",  "PART_OF",
         "PART_OF: the chemical is part of / belongs to the protein/gene entity (not usually evaluated)."),
        ("CPR:2",  "REGULATOR",
         "REGULATOR (DIRECT/INDIRECT): the chemical regulates the protein/gene (not usually evaluated)."),
        ("CPR:3",  "UPREGULATOR",
         "UPREGULATOR (includes ACTIVATOR / INDIRECT_UPREGULATOR): the chemical increases activity/expression/function of the protein/gene."),
        ("CPR:4",  "DOWNREGULATOR",
         "DOWNREGULATOR (includes INHIBITOR / INDIRECT_DOWNREGULATOR): the chemical decreases activity/expression/function of the protein/gene."),
        ("CPR:5",  "AGONIST",
         "AGONIST (incl. AGONIST-ACTIVATOR / AGONIST-INHIBITOR): the chemical is an agonist of the protein/gene target."),
        ("CPR:6",  "ANTAGONIST",
         "ANTAGONIST: the chemical is an antagonist of the protein/gene target."),
        ("CPR:7",  "MODULATOR",
         "MODULATOR (incl. MODULATOR-ACTIVATOR / MODULATOR-INHIBITOR): the chemical modulates the protein/gene target (not usually evaluated)."),
        ("CPR:8",  "COFACTOR",
         "COFACTOR: the chemical acts as a cofactor for the protein/gene (not usually evaluated)."),
        ("CPR:9",  "SUBSTRATE_OR_PRODUCT",
         "SUBSTRATE/PRODUCT_OF/SUBSTRATE_PRODUCT_OF: the chemical is a substrate and/or product of the protein/gene (enzyme) reaction."),
        ("CPR:10", "NOT",
         "NOT: explicitly no relation / negative instance between the chemical and protein/gene."),
    ]

    # slice by k (keep deterministic order)
    cpr_defs = cpr_defs[: max(0, int(k))]

    out = []
    for i, (cid, cname, cdesc) in enumerate(cpr_defs, start=1):
        out.append(
            {
                "id": cid,
                "name": cname,
                "description": cdesc,
                "rank": i,         # fixed
            }
        )

    if verbose:
        print("=== CPR CLASSES (Fixed order) ===")
        for it in out:
            print(
                f"{it['id']:6s} | {it['name']:<22s} | "
                f"final_rank={it['rank']:>2d}"
            )

    return out