from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

from feature_extractor import extract_features
from advisor.features import prepare_laptop_dataframe

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
FPT_OUTPUT_CSV = DATA_DIR / "fpt_laptops_features.csv"
FPT_RAW_JSON = DATA_DIR / "fpt_laptops.json"

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
    "GPU model",
    "GPU type",
    "Weight (kg)",
    "Battery",
    "Battery (Wh)",
    "Battery life (hours)",
    "Price (VND)",
    "Original Price (VND)",
    "Is Installment 0%",
    "Student Discount (VND)",
    "Gifts",
    "Stock Status",
    "url",
    "image",
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


def _first_present(item: Dict, *keys: str):
    feats = item.get("features") or {}
    for key in keys:
        if item.get(key) not in (None, ""):
            return item.get(key)
        if feats.get(key) not in (None, ""):
            return feats.get(key)
    return ""


def _infer_installment_0(item: Dict) -> bool:
    text = " ".join(
        str(x or "")
        for x in [
            item.get("name"),
            item.get("price_raw"),
            item.get("promotion"),
            item.get("installment"),
            item.get("url"),
        ]
    ).lower()
    return any(x in text for x in ["trả góp 0%", "tra gop 0%", "góp 0%", "gop 0%"])


def _retail_fields(item: Dict) -> Dict:
    stock = _first_present(item, "stock_status", "Stock Status")
    if not stock:
        stock = "In Stock"
    return {
        "Original Price (VND)": _first_present(item, "original_price", "Original Price (VND)"),
        "Is Installment 0%": _first_present(item, "is_installment_0", "Is Installment 0%") or _infer_installment_0(item),
        "Student Discount (VND)": _first_present(item, "student_discount", "Student Discount (VND)") or 0,
        "Gifts": _first_present(item, "gifts", "Gifts"),
        "Stock Status": stock,
        "image": item.get("image", ""),
        "source": "fpt",
    }


def build() -> None:
    if not FPT_RAW_JSON.exists():
        sys.exit(f"Missing {FPT_RAW_JSON}. Crawl FPT first or provide the raw FPT JSON.")

    all_items: List[Dict] = []
    seen_urls: set[str] = set()
    raw = _load(FPT_RAW_JSON)
    valid = 0
    for item in raw:
        url = item.get("url", "")
        if url in seen_urls:
            continue
        seen_urls.add(url)
        _re_extract(item)
        if not _is_valid(item):
            continue
        all_items.append(item)
        valid += 1
    print(f"  {'fpt':>12s}: {len(raw):>4d} raw -> {valid:>4d} valid")

    if not all_items:
        sys.exit("No valid products found.")

    # ---- FPT CSV ----
    rows: List[Dict] = []
    for item in all_items:
        feats = item.get("features") or {}
        row = {
            "Product Name": item.get("name", ""),
            "url": item.get("url", ""),
            "saved_path": item.get("saved_path", ""),
            "detail_specs_html_path": item.get("detail_specs_html_path", ""),
            **_retail_fields(item),
        }
        for col in FEATURE_COLUMNS:
            if col not in row:
                row[col] = feats.get(col, "")
        rows.append(row)

    feature_df = prepare_laptop_dataframe(pd.DataFrame(rows), fpt_only=True)

    FPT_OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(FPT_OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        fieldnames = list(dict.fromkeys(FEATURE_COLUMNS + list(feature_df.columns)))
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in feature_df.to_dict("records"):
            writer.writerow(row)

    print(f"\n  Total valid products: {len(all_items)}")
    print(f"  FPT CSV -> {FPT_OUTPUT_CSV}\n")

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
