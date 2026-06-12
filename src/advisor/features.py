from __future__ import annotations

import re
from typing import Any, Iterable, Optional

import pandas as pd


NUMERIC_COLUMNS = [
    "Price (VND)",
    "Original Price (VND)",
    "Student Discount (VND)",
    "RAM (GB)",
    "Storage (GB)",
    "Screen Size (inch)",
    "Refresh Rate (Hz)",
    "Weight (kg)",
    "CPU generation",
    "CPU Speed (GHz)",
    "Battery (Wh)",
    "Battery life (hours)",
]

BOOLEAN_TRUE = {"true", "1", "yes", "y", "co", "có", "in stock", "con hang", "còn hàng"}
KNOWN_BRANDS = [
    "asus", "acer", "dell", "hp", "lenovo", "msi", "apple", "macbook",
    "lg", "samsung", "gigabyte", "colorful", "masstel",
]


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _contains_any(value: Any, needles: Iterable[str]) -> bool:
    text = _norm_text(value).lower()
    return any(n in text for n in needles)


def _infer_brand_from_title(value: Any) -> Optional[str]:
    text = f" {str(value or '').lower()} "
    for brand in KNOWN_BRANDS:
        if f" {brand} " in text or f"{brand}-" in text:
            return "apple" if brand == "macbook" else brand
    return None


def _infer_ram_from_title(value: Any) -> Optional[int]:
    text = str(value or "").lower()
    matches = []
    for m in re.finditer(r"(?<!ssd)(?<!rom)(?<!ổ cứng)(?<!storage)\b(\d{1,3})\s*gb\b", text):
        n = int(m.group(1))
        if 4 <= n <= 64:
            matches.append(n)
    return max(matches) if matches else None


def _infer_storage_from_title(value: Any) -> Optional[int]:
    text = str(value or "").lower()
    tb = re.search(r"(\d+(?:\.\d+)?)\s*tb\b", text)
    if tb:
        return int(float(tb.group(1)) * 1000)
    for m in re.finditer(r"\b(\d{3,4})\s*gb\b", text):
        n = int(m.group(1))
        if n >= 128:
            return n
    return None


def _parse_battery_wh(value: Any) -> Optional[float]:
    text = _norm_text(value)
    match = re.search(r"(\d+(?:\.\d+)?)\s*wh", text, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _norm_series(series: pd.Series, inverse: bool = False) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    if values.notna().sum() == 0:
        out = pd.Series(0.5, index=series.index)
        return 1 - out if inverse else out

    lo = values.quantile(0.05)
    hi = values.quantile(0.95)
    denom = hi - lo
    if not denom:
        out = pd.Series(0.5, index=series.index)
    else:
        out = ((values - lo) / denom).clip(0, 1)
    out = out.fillna(0.5)
    return 1 - out if inverse else out


def _cpu_score(df: pd.DataFrame) -> pd.Series:
    gen = pd.to_numeric(df.get("CPU generation", pd.Series(0, index=df.index)), errors="coerce").fillna(0)
    speed = pd.to_numeric(df.get("CPU Speed (GHz)", pd.Series(0, index=df.index)), errors="coerce").fillna(0)
    brand = df.get("CPU brand modifier", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()

    family = pd.Series(0.35, index=df.index)
    family = family.mask(brand.str.contains("i3|ryzen\\s*3|core\\s*3", regex=True), 0.45)
    family = family.mask(brand.str.contains("i5|ryzen\\s*5|core\\s*5|ultra\\s*5", regex=True), 0.62)
    family = family.mask(brand.str.contains("i7|ryzen\\s*7|core\\s*7|ultra\\s*7", regex=True), 0.78)
    family = family.mask(brand.str.contains("i9|ryzen\\s*9|core\\s*9|ultra\\s*9", regex=True), 0.90)
    family = family.mask(brand.str.contains(r"\bm[1-5]\b", regex=True), 0.76)

    gen_score = (gen / 14).clip(0, 1)
    speed_score = (speed / 5).clip(0, 1)
    return (family * 0.55 + gen_score * 0.30 + speed_score * 0.15).clip(0, 1)


def _gpu_score(df: pd.DataFrame) -> pd.Series:
    manufacturer = df.get("GPU manufacturer", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()
    model = df.get("GPU model", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()
    gpu_type = df.get("GPU type", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()
    title = df.get("Product Name", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()
    text = manufacturer + " " + model + " " + gpu_type + " " + title

    score = pd.Series(0.45, index=df.index)
    rules = [
        (r"\brtx\s*50\d{2}\b", 1.00),
        (r"\brtx\s*40\d{2}\b", 0.95),
        (r"\brtx\s*30\d{2}\b", 0.86),
        (r"\brtx\s*20\d{2}\b", 0.74),
        (r"\bgtx\s*\d{3,4}\b", 0.68),
        (r"\bradeon\s*rx\b|\brx\s*\d{3,4}\b", 0.72),
        (r"\bintel\s*arc\b|\barc\s+[a-z]?\d{3,4}\b", 0.60),
        (r"\biris\s*xe?\b", 0.54),
        (r"\bapple\s+m[1-5]\b", 0.58),
        (r"\bdedicated\b|\bnvidia\b|\bgeforce\b", 0.62),
        (r"\btuf\b|\brog\b|\bnitro\b|\bloq\b|\blegion\b|\bvictus\b|\bkatana\b|\bcyborg\b|\bpredator\b|\bgaming\b", 0.64),
        (r"\buhd\b|\bintegrated\b|\bintel graphics\b|\bradeon graphics\b|\badreno\b", 0.48),
    ]
    for pattern, value in rules:
        matched = text.str.contains(pattern, regex=True)
        score = score.where(~matched, score.clip(lower=value))
    return score.clip(0, 1)


def _as_bool_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.lower().isin(BOOLEAN_TRUE)


def prepare_laptop_dataframe(df: pd.DataFrame, *, fpt_only: bool = True) -> pd.DataFrame:
    """Normalize raw crawler CSV into the feature surface required by the advisor."""
    df = df.copy()

    if fpt_only and "source" in df.columns:
        fpt = df["source"].fillna("").astype(str).str.lower().eq("fpt")
        if fpt.any():
            df = df[fpt].copy()

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    title = df.get("Product Name", pd.Series("", index=df.index)).fillna("").astype(str)
    if "Manufacturer" not in df.columns:
        df["Manufacturer"] = title.apply(_infer_brand_from_title)
    else:
        inferred_brand = title.apply(_infer_brand_from_title)
        missing_brand = df["Manufacturer"].isna() | df["Manufacturer"].astype(str).str.strip().eq("")
        df.loc[missing_brand, "Manufacturer"] = inferred_brand[missing_brand]

    for col, infer_fn in [("RAM (GB)", _infer_ram_from_title), ("Storage (GB)", _infer_storage_from_title)]:
        if col not in df.columns:
            df[col] = title.apply(infer_fn)
        else:
            inferred = title.apply(infer_fn)
            missing = df[col].isna()
            if missing.any():
                df.loc[missing, col] = inferred[missing]

    if "Battery (Wh)" not in df.columns and "Battery" in df.columns:
        df["Battery (Wh)"] = df["Battery"].apply(_parse_battery_wh)
    elif "Battery (Wh)" in df.columns:
        df["Battery (Wh)"] = pd.to_numeric(df["Battery (Wh)"], errors="coerce")

    if "Stock Status" not in df.columns:
        df["Stock Status"] = "In Stock"
    else:
        unknown = df["Stock Status"].isna() | df["Stock Status"].astype(str).str.strip().eq("")
        df.loc[unknown, "Stock Status"] = "In Stock"

    if "Is Installment 0%" not in df.columns:
        df["Is Installment 0%"] = title.apply(lambda x: _contains_any(x, ["trả góp 0%", "tra gop 0%", "góp 0%"]))
    else:
        df["Is Installment 0%"] = _as_bool_series(df["Is Installment 0%"])

    if "Student Discount (VND)" not in df.columns:
        df["Student Discount (VND)"] = 0
    if "Gifts" not in df.columns:
        df["Gifts"] = ""

    df["norm_ram"] = _norm_series(df.get("RAM (GB)", pd.Series(index=df.index, dtype=float)))
    df["norm_storage"] = _norm_series(df.get("Storage (GB)", pd.Series(index=df.index, dtype=float)))
    df["norm_price"] = _norm_series(df.get("Price (VND)", pd.Series(index=df.index, dtype=float)))
    df["norm_weight"] = _norm_series(df.get("Weight (kg)", pd.Series(index=df.index, dtype=float)), inverse=True)
    df["norm_screen"] = _norm_series(df.get("Screen Size (inch)", pd.Series(index=df.index, dtype=float)))
    df["norm_battery"] = _norm_series(df.get("Battery (Wh)", pd.Series(index=df.index, dtype=float)))
    battery_hours_score = _norm_series(
        df.get("Battery life (hours)", pd.Series(index=df.index, dtype=float))
    )
    battery_wh_values = pd.to_numeric(
        df.get("Battery (Wh)", pd.Series(index=df.index, dtype=float)),
        errors="coerce",
    )
    battery_hour_values = pd.to_numeric(
        df.get("Battery life (hours)", pd.Series(index=df.index, dtype=float)),
        errors="coerce",
    )

    df["gpu_score"] = _gpu_score(df)
    df["norm_cpu"] = _cpu_score(df)
    df["battery_score"] = df["norm_battery"].where(battery_wh_values.notna(), battery_hours_score)
    df["base_performance_score"] = (
        df["norm_cpu"] * 0.45 + df["gpu_score"] * 0.30 + df["norm_ram"] * 0.15 + df["norm_storage"] * 0.10
    ).clip(0, 1)

    df["gaming_score"] = (
        df["gpu_score"] * 0.60 + df["base_performance_score"] * 0.25 + df["norm_ram"] * 0.15
    ).clip(0, 1)
    df["ai_graphics_score"] = (
        df["gpu_score"] * 0.55 + df["base_performance_score"] * 0.30 + df["norm_ram"] * 0.15
    ).clip(0, 1)
    df["office_score"] = (
        df["base_performance_score"] * 0.45 + df["norm_weight"] * 0.35 + df["battery_score"].fillna(0.5) * 0.20
    ).clip(0, 1)
    df["portability_score"] = (
        df["norm_weight"] * 0.45 + df["battery_score"].fillna(0.5) * 0.35 + (1 - df["norm_screen"]) * 0.20
    ).clip(0, 1)
    df["general_score"] = (
        df["office_score"] * 0.45 + df["base_performance_score"] * 0.35 + df["portability_score"] * 0.20
    ).clip(0, 1)

    df["is_gaming_ready"] = df["gaming_score"] >= 0.60
    df["is_ai_ready"] = df["ai_graphics_score"] >= 0.60
    weight_values = pd.to_numeric(
        df.get("Weight (kg)", pd.Series(index=df.index, dtype=float)),
        errors="coerce",
    )
    weight_observed = weight_values.notna()
    battery_wh_observed = battery_wh_values.notna()
    battery_hours_observed = battery_hour_values.notna()
    battery_observed = battery_wh_observed | battery_hours_observed

    df["is_business_ready"] = df["office_score"] >= 0.55
    df["is_ultrabook"] = (
        weight_observed
        & battery_observed
        & (df["norm_weight"] >= 0.70)
        & (df["battery_score"] >= 0.55)
    )
    df["is_light"] = weight_observed & weight_values.le(1.7)
    df["is_small_screen"] = pd.to_numeric(df.get("Screen Size (inch)"), errors="coerce").le(14.1).fillna(False)
    df["is_large_screen"] = pd.to_numeric(df.get("Screen Size (inch)"), errors="coerce").ge(15.6).fillna(False)

    return df
