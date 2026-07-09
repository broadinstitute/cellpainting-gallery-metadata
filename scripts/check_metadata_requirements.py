"""
CI check: every key in harmonized_ontology.json that has a "Values" list
must appear as a backtick-quoted field in docs/metadata_requirements.md.
Exits 1 if any are missing.
"""
import json
import re
import pathlib
import sys

ROOT = pathlib.Path(__file__).parent.parent
ontology = json.loads((ROOT / "harmonized_ontology.json").read_text())
md = (ROOT / "docs" / "metadata_requirements.md").read_text()
md_fields = set(re.findall(r'^`([^`]+)`', md, re.MULTILINE))

missing = [k for k in ontology if k not in md_fields]

if missing:
    print("ERROR: The following keys in harmonized_ontology.json are missing")
    print("from docs/metadata_requirements.md:")
    for k in missing:
        print(f"  {k}")
    print("\nAdd them to docs/metadata_requirements.md and re-run")
    print("scripts/generate_metadata_requirements.py to fill in any allowed values.")
    sys.exit(1)

print(f"OK: all {len(ontology)} keys are documented in metadata_requirements.md")
