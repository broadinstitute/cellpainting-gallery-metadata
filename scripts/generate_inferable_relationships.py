"""
Generates docs/inferable_relationships.md from inferable_relationships.json.
Run before building the docs site.
"""
import json
import pathlib

ROOT = pathlib.Path(__file__).parent.parent
data = json.loads((ROOT / "inferable_relationships.json").read_text())

lines = [
    "# Inferable Relationships",
    "",
    "Some metadata fields can be inferred from other fields.",
    "If you provide the key field, the values below can be filled in automatically.",
    "",
]

for top_key, entries in data.items():
    if top_key == "Label_Alternative_Names":
        continue

    lines.append(f"## {top_key}")
    lines.append("")

    # Collect all sub-field names from the entries
    sub_fields = list({f for v in entries.values() for f in v})
    sub_fields.sort()

    header = f"| {top_key} | " + " | ".join(sub_fields) + " |"
    sep = "| --- | " + " | ".join("---" for _ in sub_fields) + " |"
    lines.append(header)
    lines.append(sep)

    for key, values in entries.items():
        row = f"| {key} | " + " | ".join(str(values.get(f, "")) for f in sub_fields) + " |"
        lines.append(row)

    lines.append("")

out_path = ROOT / "docs" / "inferable_relationships.md"
out_path.write_text("\n".join(lines))
print(f"Generated {out_path}")
