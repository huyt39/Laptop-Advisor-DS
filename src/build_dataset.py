from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Dict, List

from az_no_db import extract_features

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_CSV = DATA_DIR / "all_laptops.csv"
PARSED_DIR = DATA_DIR / "parsed_specs"

FEATURE_COLUMNS = [
    "Product Name",
    "Manufacturer",
    "CPU manufacturer",
    "CPU brand modifier",
    "CPU generation",
    "CPU Speed (GHz)",
    "RAM (GB)",
    "RAM Type",
    "Bus (MHz)",
    "Storage (GB)",
    "Screen Size (inch)",
    "Screen Resolution",
    "Refresh Rate (Hz)",
    "GPU manufacturer",
    "Weight (kg)",
    "Battery",
    "Price (VND)",
    "url",
    "source",
    "saved_path",
    "detail_specs_html_path",
]


def _load(path: Path) -> List[Dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"  [warn] Cannot read {path.name}: {exc}")
        return []


def _re_extract(item: Dict) -> Dict:
    """Re-run extract_features() with the latest logic so the dataset
    always reflects the current normalisation rules."""
    name = item.get("name", "")
    specs = item.get("specs") or {}
    price = item.get("price")
    item["features"] = extract_features(name, specs, price)
    return item


def _is_valid(item: Dict) -> bool:
    """Keep entries that have a name, a price, and at least 2 meaningful features."""
    if not item.get("name"):
        return False
    feats = item.get("features") or {}
    if not feats.get("Price (VND)"):
        return False
    filled = sum(
        1
        for k, v in feats.items()
        if v and k not in ("Price (VND)", "Manufacturer")
    )
    return filled >= 2


def build() -> None:
    shop_files = sorted(DATA_DIR.glob("*_laptops.json"))
    if not shop_files:
        sys.exit("No *_laptops.json files found in data/.")

    all_items: List[Dict] = []
    seen_urls: set[str] = set()

    for sf in shop_files:
        shop = sf.stem.replace("_laptops", "")
        raw = _load(sf)
        valid = 0
        for item in raw:
            url = item.get("url", "")
            if url in seen_urls:
                continue
            seen_urls.add(url)
            _re_extract(item)
            if not _is_valid(item):
                continue
            item["_source"] = shop
            all_items.append(item)
            valid += 1
        print(f"  {shop:>12s}: {len(raw):>4d} raw -> {valid:>4d} valid")

    if not all_items:
        sys.exit("No valid products found.")

    # ---- Per-product JSON ----
    for idx, item in enumerate(all_items):
        shop = item["_source"]
        shop_dir = PARSED_DIR / shop
        shop_dir.mkdir(parents=True, exist_ok=True)
        out_data = {
            "Product Name": item.get("name", ""),
            **(item.get("features") or {}),
            "url": item.get("url", ""),
            "saved_path": item.get("saved_path", ""),
            "detail_specs_html_path": item.get("detail_specs_html_path", ""),
        }
        fp = shop_dir / f"{idx:04d}.json"
        fp.write_text(json.dumps(out_data, ensure_ascii=False, indent=2), encoding="utf-8")

    # ---- Consolidated CSV ----
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FEATURE_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for item in all_items:
            feats = item.get("features") or {}
            row = {
                "Product Name": item.get("name", ""),
                "url": item.get("url", ""),
                "source": item["_source"],
                "saved_path": item.get("saved_path", ""),
                "detail_specs_html_path": item.get("detail_specs_html_path", ""),
            }
            for col in FEATURE_COLUMNS:
                if col not in row:
                    row[col] = feats.get(col, "")
            writer.writerow(row)

    print(f"\n  Total valid products: {len(all_items)}")
    print(f"  CSV  -> {OUTPUT_CSV}")
    print(f"  JSON -> {PARSED_DIR}/\n")

    # ---- Quality stats ----
    print("  --- Column fill rates ---")
    for col in FEATURE_COLUMNS[:17]:
        if col == "Product Name":
            count = sum(1 for it in all_items if it.get("name"))
        else:
            count = sum(1 for it in all_items if (it.get("features") or {}).get(col))
        pct = count / len(all_items) * 100
        bar = "#" * int(pct / 5)
        print(f"  {col:<22s} {count:>5d}/{len(all_items):<5d} ({pct:5.1f}%) {bar}")


if __name__ == "__main__":
    build()
