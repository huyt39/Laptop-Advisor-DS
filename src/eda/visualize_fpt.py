from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd


PRICE_COL = "Price (VND)"
RAM_COL = "RAM (GB)"
STORAGE_COL = "Storage (GB)"
BRAND_COL = "Manufacturer"
GPU_COL = "GPU manufacturer"
GPU_MODEL_COL = "GPU model"
GPU_TYPE_COL = "GPU type"
SCREEN_COL = "Screen Size (inch)"


def _money_million(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce") / 1_000_000


def _save(fig: plt.Figure, out_dir: Path, name: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out_dir / name, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _format_brand(value: object) -> str:
    text = str(value).strip()
    return text.title() if text else "Thiếu"


def _format_storage(value: object) -> str:
    if pd.isna(value):
        return "Thiếu"
    number = float(value)
    if number >= 1000:
        tb = number / 1000
        return f"{tb:g}TB"
    return f"{number:g}GB"


def _price_segment(price: float) -> str:
    if pd.isna(price):
        return "Thiếu"
    if price < 15_000_000:
        return "Giá rẻ"
    if price < 25_000_000:
        return "Tầm trung"
    if price < 40_000_000:
        return "Cận cao cấp"
    return "Cao cấp"


def fig8_overview(df: pd.DataFrame, out_dir: Path) -> None:
    cols = [
        "Product Name",
        BRAND_COL,
        "CPU manufacturer",
        RAM_COL,
        STORAGE_COL,
        "Screen Size (inch)",
        GPU_COL,
        "Weight (kg)",
        "Battery",
        PRICE_COL,
    ]
    available = [c for c in cols if c in df.columns]
    rates = df[available].notna().mean().mul(100).sort_values()
    labels = {
        "Product Name": "Tên",
        BRAND_COL: "Hãng",
        "CPU manufacturer": "CPU",
        RAM_COL: "RAM",
        STORAGE_COL: "Ổ cứng",
        "Screen Size (inch)": "Màn hình",
        GPU_COL: "GPU",
        "Weight (kg)": "Cân nặng",
        "Battery": "Pin",
        PRICE_COL: "Giá",
    }

    fig, ax = plt.subplots(figsize=(8, 4.8))
    ax.barh([labels.get(c, c) for c in rates.index], rates.values, color="#2f6f73")
    ax.set_title("Tỷ lệ dữ liệu có giá trị")
    ax.set_xlabel("Tỷ lệ (%)")
    ax.set_xlim(0, 105)
    ax.grid(axis="x", alpha=0.25)
    for i, value in enumerate(rates.values):
        ax.text(value + 1, i, f"{value:.1f}%", va="center", fontsize=8)
    _save(fig, out_dir, "fig8_tong_quan_dataset.png")


def fig9_price_distribution(df: pd.DataFrame, out_dir: Path) -> None:
    prices = _money_million(df[PRICE_COL]).dropna()
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.hist(prices, bins=24, color="#3973b7", edgecolor="white")
    ax.set_title("Phân phối giá laptop")
    ax.set_xlabel("Giá (triệu VND)")
    ax.set_ylabel("Số SKU")
    ax.grid(axis="y", alpha=0.25)
    _save(fig, out_dir, "fig9_phan_phoi_gia.png")


def fig10_price_segments(df: pd.DataFrame, out_dir: Path) -> None:
    segments = df[PRICE_COL].apply(_price_segment)
    order = ["Giá rẻ", "Tầm trung", "Cận cao cấp", "Cao cấp", "Thiếu"]
    counts = segments.value_counts().reindex(order).dropna()
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.bar(counts.index, counts.values, color="#5b8c5a")
    ax.set_title("Số laptop theo phân khúc giá")
    ax.set_xlabel("Phân khúc")
    ax.set_ylabel("Số SKU")
    ax.grid(axis="y", alpha=0.25)
    _save(fig, out_dir, "fig10_phan_khuc_gia.png")


def fig11_brand_counts(df: pd.DataFrame, out_dir: Path) -> None:
    counts = df[BRAND_COL].map(_format_brand).value_counts().head(12).sort_values()
    fig, ax = plt.subplots(figsize=(8, 4.8))
    ax.barh(counts.index, counts.values, color="#7c5aa6")
    ax.set_title("Số SKU theo hãng")
    ax.set_xlabel("Số SKU")
    ax.set_ylabel("Hãng")
    ax.grid(axis="x", alpha=0.25)
    _save(fig, out_dir, "fig11_so_mau_theo_hang.png")


def fig12_common_configs(df: pd.DataFrame, out_dir: Path) -> None:
    work = df[[RAM_COL, STORAGE_COL, GPU_COL]].copy()
    work["RAM"] = pd.to_numeric(work[RAM_COL], errors="coerce").map(
        lambda x: "Thiếu" if pd.isna(x) else f"{x:g}GB RAM"
    )
    work["Ổ cứng"] = pd.to_numeric(work[STORAGE_COL], errors="coerce").map(_format_storage)
    work["GPU"] = work[GPU_COL].fillna("Thiếu")
    configs = (work["RAM"] + " / " + work["Ổ cứng"] + " / " + work["GPU"]).value_counts().head(10).sort_values()

    fig, ax = plt.subplots(figsize=(9, 5.2))
    ax.barh(configs.index, configs.values, color="#c77b30")
    ax.set_title("Top cấu hình RAM, ổ cứng và hãng GPU")
    ax.set_xlabel("Số SKU")
    ax.set_ylabel("Cấu hình")
    ax.grid(axis="x", alpha=0.25)
    _save(fig, out_dir, "fig12_cau_hinh_pho_bien.png")


def fig13_price_by_ram(df: pd.DataFrame, out_dir: Path) -> None:
    work = df[[RAM_COL, PRICE_COL]].copy()
    work[RAM_COL] = pd.to_numeric(work[RAM_COL], errors="coerce")
    work["Giá"] = _money_million(work[PRICE_COL])
    work = work.dropna()
    ram_order = sorted(work[RAM_COL].unique())
    data = [work.loc[work[RAM_COL] == ram, "Giá"] for ram in ram_order]

    fig, ax = plt.subplots(figsize=(8, 4.8))
    ax.boxplot(data, tick_labels=[f"{ram:g}GB" for ram in ram_order], showfliers=False)
    ax.set_title("Giá theo dung lượng RAM")
    ax.set_xlabel("RAM")
    ax.set_ylabel("Giá (triệu VND)")
    ax.grid(axis="y", alpha=0.25)
    _save(fig, out_dir, "fig13_gia_theo_ram.png")


def fig14_gpu_counts(df: pd.DataFrame, out_dir: Path) -> None:
    labels = {
        "Integrated": "GPU tích hợp",
        "Dedicated": "GPU rời",
    }
    counts = df[GPU_TYPE_COL].map(labels).fillna("Chưa xác định").value_counts().sort_values()
    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    ax.barh(counts.index, counts.values, color="#4c7c9c")
    ax.set_title("Số SKU theo loại GPU")
    ax.set_xlabel("Số SKU")
    ax.set_ylabel("Loại GPU")
    ax.grid(axis="x", alpha=0.25)
    _save(fig, out_dir, "fig14_nhom_gpu.png")


def fig15_gpu_models(df: pd.DataFrame, out_dir: Path) -> None:
    counts = df[GPU_MODEL_COL].dropna().value_counts().head(10).sort_values()
    fig, ax = plt.subplots(figsize=(8.5, 5))
    ax.barh(counts.index, counts.values, color="#9a5b72")
    ax.set_title("Top 10 model GPU phổ biến")
    ax.set_xlabel("Số SKU")
    ax.set_ylabel("Model GPU")
    ax.grid(axis="x", alpha=0.25)
    _save(fig, out_dir, "fig15_model_gpu_pho_bien.png")


def fig16_price_by_gpu(df: pd.DataFrame, out_dir: Path) -> None:
    work = df[[GPU_MODEL_COL, PRICE_COL]].copy()
    work["Giá"] = _money_million(work[PRICE_COL])
    work = work.dropna()
    top_models = work[GPU_MODEL_COL].value_counts().head(10).index
    work = work[work[GPU_MODEL_COL].isin(top_models)]
    gpu_order = work.groupby(GPU_MODEL_COL)["Giá"].median().sort_values().index.tolist()
    data = [work.loc[work[GPU_MODEL_COL] == gpu, "Giá"] for gpu in gpu_order]

    fig, ax = plt.subplots(figsize=(10, 5.2))
    ax.boxplot(data, tick_labels=gpu_order, showfliers=False)
    ax.set_title("Giá theo Top 10 model GPU")
    ax.set_xlabel("Model GPU")
    ax.set_ylabel("Giá (triệu VND)")
    ax.tick_params(axis="x", rotation=35)
    ax.grid(axis="y", alpha=0.25)
    _save(fig, out_dir, "fig16_gia_theo_gpu.png")


def _screen_groups(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return pd.cut(
        values,
        bins=[0, 14, 15.6, float("inf")],
        labels=["Tối đa 14 inch", "Trên 14 đến 15.6 inch", "Trên 15.6 inch"],
        include_lowest=True,
    )


def fig17_screen_price_tradeoff(df: pd.DataFrame, out_dir: Path) -> None:
    work = df[[SCREEN_COL, PRICE_COL, GPU_TYPE_COL]].copy()
    work[SCREEN_COL] = pd.to_numeric(work[SCREEN_COL], errors="coerce")
    work["Giá"] = _money_million(work[PRICE_COL])
    work["Loại GPU"] = work[GPU_TYPE_COL].map(
        {"Integrated": "GPU tích hợp", "Dedicated": "GPU rời"}
    ).fillna("Chưa xác định")
    work = work.dropna(subset=[SCREEN_COL, "Giá"])

    colors = {"GPU tích hợp": "#4c7c9c", "GPU rời": "#c46d3b", "Chưa xác định": "#777777"}
    fig, ax = plt.subplots(figsize=(8.5, 5))
    for label, group in work.groupby("Loại GPU"):
        ax.scatter(
            group[SCREEN_COL],
            group["Giá"],
            label=label,
            color=colors[label],
            alpha=0.55,
            s=28,
        )
    ax.set_title("Kích thước màn hình và giá")
    ax.set_xlabel("Kích thước màn hình (inch)")
    ax.set_ylabel("Giá (triệu VND)")
    ax.legend(title="Loại GPU")
    ax.grid(alpha=0.2)
    _save(fig, out_dir, "fig17_man_hinh_va_gia.png")


def fig18_screen_group_tradeoff(df: pd.DataFrame, out_dir: Path) -> None:
    work = df[[SCREEN_COL, PRICE_COL, GPU_TYPE_COL]].copy()
    work["Nhóm màn hình"] = _screen_groups(work[SCREEN_COL])
    work["Giá"] = _money_million(work[PRICE_COL])
    work["GPU rời"] = work[GPU_TYPE_COL].eq("Dedicated")
    summary = work.dropna(subset=["Nhóm màn hình", "Giá"]).groupby(
        "Nhóm màn hình", observed=True
    ).agg(
        sku=("Giá", "size"),
        gia_trung_vi=("Giá", "median"),
        ty_le_gpu_roi=("GPU rời", "mean"),
    )

    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.5))
    axes[0].bar(summary.index.astype(str), summary["gia_trung_vi"], color="#5b8c5a")
    axes[0].set_title("Giá trung vị")
    axes[0].set_ylabel("Triệu VND")
    axes[0].tick_params(axis="x", rotation=18)
    axes[0].grid(axis="y", alpha=0.25)

    percent = summary["ty_le_gpu_roi"] * 100
    axes[1].bar(summary.index.astype(str), percent, color="#c46d3b")
    axes[1].set_title("Tỷ lệ GPU rời")
    axes[1].set_ylabel("Tỷ lệ (%)")
    axes[1].tick_params(axis="x", rotation=18)
    axes[1].grid(axis="y", alpha=0.25)
    for i, (value, sku) in enumerate(zip(percent, summary["sku"])):
        axes[1].text(i, value + 1, f"{value:.1f}%\n(n={sku})", ha="center", fontsize=8)

    fig.suptitle("Đánh đổi theo nhóm màn hình")
    _save(fig, out_dir, "fig18_danh_doi_man_hinh.png")


def main() -> None:
    parser = argparse.ArgumentParser(description="Tạo biểu đồ EDA cho dataset laptop FPT.")
    parser.add_argument("--csv", default="data/fpt_laptops_features.csv", help="Đường dẫn feature CSV.")
    parser.add_argument("--out-dir", default="reports/figures", help="Thư mục lưu hình PNG.")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    out_dir = Path(args.out_dir)

    fig8_overview(df, out_dir)
    fig9_price_distribution(df, out_dir)
    fig10_price_segments(df, out_dir)
    fig11_brand_counts(df, out_dir)
    fig12_common_configs(df, out_dir)
    fig13_price_by_ram(df, out_dir)
    fig14_gpu_counts(df, out_dir)
    fig15_gpu_models(df, out_dir)
    fig16_price_by_gpu(df, out_dir)
    fig17_screen_price_tradeoff(df, out_dir)
    fig18_screen_group_tradeoff(df, out_dir)

    print(f"Saved EDA figures to {out_dir}")


if __name__ == "__main__":
    main()
