from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from feature_extractor import extract_features


BASE_COLUMNS = [
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
]

DISPLAY_NAMES = {
    "Product Name": "Tên SKU",
    "Manufacturer": "Hãng",
    "CPU manufacturer": "Hãng CPU",
    "CPU brand modifier": "Dòng CPU",
    "CPU generation": "Thế hệ CPU",
    "CPU Speed (GHz)": "Xung CPU",
    "RAM (GB)": "RAM",
    "RAM Type": "Loại RAM",
    "Bus (MHz)": "Bus RAM",
    "Storage (GB)": "Ổ cứng",
    "Screen Size (inch)": "Kích thước màn hình",
    "Screen Resolution": "Độ phân giải",
    "Refresh Rate (Hz)": "Tần số quét",
    "GPU manufacturer": "Hãng GPU",
    "GPU model": "Model GPU",
    "GPU type": "Loại GPU",
    "Weight (kg)": "Cân nặng",
    "Battery": "Thông tin pin",
    "Battery (Wh)": "Pin Wh",
    "Battery life (hours)": "Thời lượng pin",
    "Price (VND)": "Giá bán",
    "Original Price (VND)": "Giá gốc",
    "Is Installment 0%": "Trả góp 0%",
    "Student Discount (VND)": "Ưu đãi HSSV",
    "Gifts": "Quà tặng",
    "Stock Status": "Tồn kho",
    "url": "URL",
    "image": "Ảnh",
}

RETAIL_SOURCE_KEYS = {
    "Original Price (VND)": "original_price",
    "Is Installment 0%": "is_installment_0",
    "Student Discount (VND)": "student_discount",
    "Gifts": "gifts",
    "Stock Status": "stock_status",
}


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _load_raw(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON array")
    return data


def _before_dataframe(raw: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in raw:
        features = extract_features(
            str(item.get("name") or ""),
            item.get("specs") or {},
            item.get("price"),
        )
        row = {
            "Product Name": item.get("name"),
            "url": item.get("url"),
            "image": item.get("image"),
            "Original Price (VND)": item.get("original_price"),
            "Is Installment 0%": item.get("is_installment_0"),
            "Student Discount (VND)": item.get("student_discount"),
            "Gifts": item.get("gifts"),
            "Stock Status": item.get("stock_status"),
            **features,
        }
        rows.append(row)
    return pd.DataFrame(rows).reindex(columns=BASE_COLUMNS)


def _missing_mask(series: pd.Series) -> pd.Series:
    return series.isna() | series.astype(str).str.strip().isin({"", "nan", "None"})


def _missing_table(before: pd.DataFrame, after: pd.DataFrame) -> pd.DataFrame:
    rows = []
    total = len(before)
    for column in BASE_COLUMNS:
        before_series = before[column] if column in before else pd.Series(index=before.index, dtype=object)
        after_series = after[column] if column in after else pd.Series(index=after.index, dtype=object)
        before_missing = int(_missing_mask(before_series).sum())
        after_missing = int(_missing_mask(after_series).sum())
        rows.append(
            {
                "column": column,
                "label_vi": DISPLAY_NAMES.get(column, column),
                "missing_before": before_missing,
                "missing_before_pct": round(before_missing / total * 100, 1),
                "missing_after": after_missing,
                "missing_after_pct": round(after_missing / len(after) * 100, 1),
            }
        )
    return pd.DataFrame(rows)


def _invalid_numeric_counts(df: pd.DataFrame) -> dict[str, int]:
    checks = {
        "Giá ngoài 3–300 triệu": ("Price (VND)", 3_000_000, 300_000_000),
        "RAM ngoài 4–128GB": ("RAM (GB)", 4, 128),
        "Ổ cứng ngoài 128–8192GB": ("Storage (GB)", 128, 8192),
        "Màn hình ngoài 10–20 inch": ("Screen Size (inch)", 10, 20),
        "Cân nặng ngoài 0.5–6kg": ("Weight (kg)", 0.5, 6),
        "Pin ngoài 20–150Wh": ("Battery (Wh)", 20, 150),
    }
    result = {}
    for label, (column, low, high) in checks.items():
        values = pd.to_numeric(df.get(column), errors="coerce")
        result[label] = int((values.notna() & ~values.between(low, high)).sum())
    return result


def _retail_availability(raw: list[dict[str, Any]], after: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for column, raw_key in RETAIL_SOURCE_KEYS.items():
        source_count = sum(_present(item.get(raw_key)) for item in raw)
        stored_count = int((~_missing_mask(after[column])).sum()) if column in after else 0
        rows.append(
            {
                "field": column,
                "source_count": source_count,
                "source_pct": round(source_count / len(raw) * 100, 1),
                "stored_non_missing": stored_count,
                "note": "Fallback/schema default" if source_count == 0 and stored_count else "",
            }
        )
    return pd.DataFrame(rows)


def _issue_table(
    before: pd.DataFrame,
    after: pd.DataFrame,
    missing: pd.DataFrame,
    invalid: dict[str, int],
) -> pd.DataFrame:
    missing_by_col = missing.set_index("column")["missing_before"].to_dict()
    repeated_names = int(before["Product Name"].duplicated(keep="first").sum())
    duplicate_urls = int(before["url"].duplicated(keep="first").sum())
    missing_required = int(
        (
            _missing_mask(before["Product Name"])
            | _missing_mask(before["url"])
            | _missing_mask(before["Price (VND)"])
        ).sum()
    )
    invalid_total = sum(invalid.values())
    return pd.DataFrame(
        [
            {
                "problem": "Trùng URL SKU",
                "count": duplicate_urls,
                "method": "Khử trùng lặp theo URL trước khi build CSV",
                "decision": "Xóa bản ghi trùng URL",
            },
            {
                "problem": "Tên sản phẩm lặp",
                "count": repeated_names,
                "method": "Kiểm tra theo URL/SKU thay vì xóa theo tên",
                "decision": "Giữ lại vì có thể là màu/cấu hình SKU khác nhau",
            },
            {
                "problem": "Thiếu trường bắt buộc",
                "count": missing_required,
                "method": "Loại dòng thiếu tên, URL hoặc giá",
                "decision": "Không có dòng bị loại trong lần build hiện tại",
            },
            {
                "problem": "Thiếu hãng CPU",
                "count": missing_by_col.get("CPU manufacturer", 0),
                "method": "Suy luận từ CPU spec và tên SKU; giữ thiếu nếu không chắc chắn",
                "decision": "Không điền giá trị đoán tùy tiện",
            },
            {
                "problem": "Thiếu Storage",
                "count": missing_by_col.get("Storage (GB)", 0),
                "method": "Suy luận từ tên SKU, chuẩn hóa GB/TB",
                "decision": "Giữ thiếu nếu không tìm thấy tín hiệu",
            },
            {
                "problem": "Thiếu GPU",
                "count": missing_by_col.get("GPU model", 0),
                "method": "Kết hợp Card đồ hoạ, tên SKU và CPU/SoC",
                "decision": "Chuẩn hóa hãng, model và loại GPU",
            },
            {
                "problem": "Thiếu cân nặng",
                "count": missing_by_col.get("Weight (kg)", 0),
                "method": "Tìm trong spec và thông số nổi bật; fallback trung lập khi scoring",
                "decision": "Không nội suy từ dòng máy khác",
            },
            {
                "problem": "Thiếu pin",
                "count": missing_by_col.get("Battery", 0),
                "method": "Tách riêng Wh và thời lượng giờ từ thông số nổi bật",
                "decision": "Không quy đổi giờ sang Wh",
            },
            {
                "problem": "Giá trị numeric ngoài miền",
                "count": invalid_total,
                "method": "Ép kiểu numeric và kiểm tra miền hợp lý",
                "decision": "Loại hoặc giữ thiếu nếu vi phạm",
            },
            {
                "problem": "Retail chưa có nguồn thật",
                "count": len(before),
                "method": "Duy trì schema bằng fallback nhưng đánh dấu giới hạn",
                "decision": "Không dùng mặc định để xác nhận ưu đãi/tồn kho",
            },
        ]
    )


def _plot_missing(missing: pd.DataFrame, out_dir: Path) -> None:
    selected = [
        "CPU manufacturer",
        "CPU generation",
        "RAM Type",
        "Bus (MHz)",
        "Storage (GB)",
        "Screen Resolution",
        "Refresh Rate (Hz)",
        "GPU model",
        "Weight (kg)",
        "Battery",
    ]
    chart = missing[missing["column"].isin(selected)].copy()
    chart = chart.sort_values("missing_before_pct")
    y = list(range(len(chart)))
    height = 0.38

    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.barh(
        [value - height / 2 for value in y],
        chart["missing_before_pct"],
        height=height,
        color="#b85c5c",
        label="Trước xử lý",
    )
    ax.barh(
        [value + height / 2 for value in y],
        chart["missing_after_pct"],
        height=height,
        color="#3973b7",
        label="Sau xử lý",
    )
    ax.set_yticks(y, chart["label_vi"])
    ax.set_xlabel("Tỷ lệ thiếu (%)")
    ax.set_title("Tỷ lệ thiếu trước và sau tiền xử lý")
    ax.set_xlim(0, 105)
    ax.legend()
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(out_dir / "fig19_missing_truoc_sau.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_issues(issues: pd.DataFrame, out_dir: Path) -> None:
    chart = issues[issues["problem"] != "Retail chưa có nguồn thật"].sort_values("count")
    fig, ax = plt.subplots(figsize=(9, 5.2))
    ax.barh(chart["problem"], chart["count"], color="#7c5aa6")
    ax.set_xlabel("Số SKU")
    ax.set_title("Các vấn đề chất lượng dữ liệu")
    ax.grid(axis="x", alpha=0.25)
    for i, value in enumerate(chart["count"]):
        ax.text(value + 3, i, str(value), va="center", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_dir / "fig20_van_de_chat_luong.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit chất lượng dữ liệu trước và sau preprocessing.")
    parser.add_argument("--raw", default="data/fpt_laptops.json")
    parser.add_argument("--csv", default="data/fpt_laptops_features.csv")
    parser.add_argument("--out-dir", default="reports/preprocessing")
    parser.add_argument("--fig-dir", default="reports/figures")
    args = parser.parse_args()

    raw_path = Path(args.raw)
    csv_path = Path(args.csv)
    out_dir = Path(args.out_dir)
    fig_dir = Path(args.fig_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    fig_dir.mkdir(parents=True, exist_ok=True)

    raw = _load_raw(raw_path)
    before = _before_dataframe(raw)
    after = pd.read_csv(csv_path)
    missing = _missing_table(before, after)
    invalid = _invalid_numeric_counts(after)
    retail = _retail_availability(raw, after)
    issues = _issue_table(before, after, missing, invalid)

    missing.to_csv(out_dir / "missing_values.csv", index=False)
    retail.to_csv(out_dir / "retail_availability.csv", index=False)
    issues.to_csv(out_dir / "quality_issues.csv", index=False)

    summary = {
        "raw_rows": len(before),
        "feature_rows": len(after),
        "feature_columns": len(after.columns),
        "unique_urls": int(before["url"].nunique()),
        "duplicate_url_rows": int(before["url"].duplicated().sum()),
        "unique_product_names": int(before["Product Name"].nunique()),
        "repeated_name_rows": int(before["Product Name"].duplicated().sum()),
        "exact_duplicate_rows": int(before.duplicated().sum()),
        "missing_required_rows": int(
            (
                _missing_mask(before["Product Name"])
                | _missing_mask(before["url"])
                | _missing_mask(before["Price (VND)"])
            ).sum()
        ),
        "invalid_numeric": invalid,
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    _plot_missing(missing, fig_dir)
    _plot_issues(issues, fig_dir)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"Saved audit tables to {out_dir}")
    print(f"Saved audit figures to {fig_dir}")


if __name__ == "__main__":
    main()
