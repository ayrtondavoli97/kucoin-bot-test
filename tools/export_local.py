"""Unisce gli item del dataset locale (storage/datasets/default) in un solo file JSON.

Uso (dalla cartella della repo, dopo `python -m src`):
    python tools/export_local.py backfill.json
"""
import json
import sys
from pathlib import Path

src = Path("storage/datasets/default")
out = Path(sys.argv[1] if len(sys.argv) > 1 else "backfill.json")

files = sorted(p for p in src.glob("*.json") if not p.name.startswith("__"))
items = [json.loads(p.read_text(encoding="utf-8")) for p in files]
out.write_text(json.dumps(items, separators=(",", ":")), encoding="utf-8")
print(f"{len(items)} item -> {out} ({out.stat().st_size / 1e6:.1f} MB)")
