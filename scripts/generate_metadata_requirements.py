"""
Syncs docs/metadata_requirements.md from harmonized_ontology.json:
- Updates field descriptions (the parenthetical after each field name)
- Fills in "Currently allowed values are:" lines
Safe to re-run — output is idempotent.
"""
import json
import re
import pathlib

ROOT = pathlib.Path(__file__).parent.parent
ontology = json.loads((ROOT / "harmonized_ontology.json").read_text())
md_path = ROOT / "docs" / "metadata_requirements.md"
lines = md_path.read_text().splitlines(keepends=True)

current_field = None
out = []
for line in lines:
    field_match = re.match(r'^(`[^`]+`)(.*)\n', line)
    if field_match:
        current_field = field_match.group(1)[1:-1]  # strip backticks
        entry = ontology.get(current_field, {})
        if "Description" in entry:
            rest = field_match.group(2)
            # replace existing parenthetical description or append one
            rest = re.sub(r'\s*\(.*\)', '', rest)
            line = f"{field_match.group(1)} ({entry['Description']}){rest}\n"

    values_match = re.match(r'(\s*Currently allowed values are:)', line)
    if values_match and current_field and "Values" in ontology.get(current_field, {}):
        values = ontology[current_field]["Values"]
        line = f"{values_match.group(1)} {', '.join(str(v) for v in values)}\n"

    out.append(line)

md_path.write_text("".join(out))
print(f"Updated {md_path}")
