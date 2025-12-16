# ============================================
# Pattern Class (Only One Base Class Needed)
# ============================================

class Pattern:
    def __init__(self, pattern_name, description, fields):
        """
        pattern_name : str
        description  : str
        fields       : list of required JSON keys (except evidence_sentence)
        """
        self.pattern_name = pattern_name
        self.description = description
        self.fields = fields

    def schema(self):
        """
        Generates an empty JSON schema for the pattern.
        """
        return {
            "pattern": self.pattern_name,
            **{field: "" for field in self.fields},
            "evidence_sentence": ""
        }


# ============================================
# Pattern Registry (Extend by Adding New Items)
# ============================================

PATTERN_LIST = [

    Pattern(
        pattern_name="gene_regulates_GO",
        description="A gene in a specific species regulates a biological process (GO term).",
        fields=["gene_name", "species", "go_term"]
    ),

    Pattern(
        pattern_name="protein_regulates_protein",
        description="One protein regulates the activity or function of another protein.",
        fields=["regulator_protein", "target_protein", "regulation_type", "species"]
    ),

    Pattern(
        pattern_name="protein_binds_protein_to_modulate_protein",
        description="Protein A binds Protein C, and this interaction modulates Protein B.",
        fields=["protein_A", "protein_C", "protein_B", "modulation_type", "species"]
    ),

    Pattern(
        pattern_name="RNA_regulates_protein",
        description="An RNA molecule directly regulates a protein’s activity.",
        fields=["rna_name_or_type", "target_protein", "regulation_type", "species"]
    ),

    Pattern(
        pattern_name="protein_belongs_to_GO",
        description="A protein is experimentally shown to possess a specific GO molecular function.",
        fields=["protein_name", "go_mf_term", "species"]
    ),

    Pattern(
        pattern_name="RNA_encodes_protein",
        description="An RNA fragment is the functional mRNA encoding a specific protein.",
        fields=["rna_name_or_fragment", "encoded_protein", "species"]
    )
]


# ============================================
# Prompt Builder
# ============================================

def build_prompt(abstract: str) -> str:
    """
    Build a prompt for LLM classification + structured extraction.
    """

    pattern_desc = "\n".join(
        [
            f"- {p.pattern_name}: {p.description} | Required fields: {p.fields}"
            for p in PATTERN_LIST
        ]
    )

    prompt = f"""
You are a biomedical relation extraction model.

Below are the possible patterns:
{pattern_desc}

TASK:
1. Determine which ONE pattern best matches the abstract.
2. Output a JSON object strictly following that pattern's required fields.
3. Values must be precise and unambiguous.
4. Add an exact 'evidence_sentence' copied from the abstract.
5. If no pattern fits, output: {{"pattern": "none"}}

OUTPUT RULE:
- Output ONLY JSON. No explanation.

ABSTRACT:
\"\"\"{abstract}\"\"\"
"""

    return prompt


# ============================================
# Extraction Function Using llm.query(text)
# ============================================

def extract_pattern(abstract: str, llm):
    """
    Passes the constructed prompt to llm.query(text) and returns the model's output.

    Example:
        result = extract_pattern(abstract_text, llm)
        print(result)
    """
    prompt = build_prompt(abstract)
    response = llm.query(prompt)
    return response


# ============================================
# OPTIONAL: Example Usage
# ============================================

if __name__ == "__main__":
    class DummyLLM:
        def query(self, text):
            print(">>> LLM received prompt:")
            print(text)
            return '{"pattern": "none"}'

    llm = DummyLLM()

    abstract = "This is a fake abstract for testing pattern extraction."
    result = extract_pattern(abstract, llm)
    print(">>> LLM Output:")
    print(result)