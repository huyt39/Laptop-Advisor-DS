from __future__ import annotations

import re
import pandas as pd
from typing import Any, Dict, Optional

from src.advisor.utils import normalize_user_types


def _norm_brand(s: str) -> str:
    text = (s or "").strip().lower()
    for brand in ["asus", "acer", "dell", "hp", "lenovo", "msi", "apple", "macbook", "lg", "samsung", "gigabyte"]:
        if brand in text:
            return "apple" if brand == "macbook" else brand
    return text


def _parse_battery_wh(battery_text: Any) -> Optional[float]:
    if battery_text is None:
        return None
    txt = str(battery_text)
    m = re.search(r"(\d+(?:\.\d+)?)\s*Wh", txt, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def apply_scoring(df, query):
    df = df.copy()
    user_types = normalize_user_types(query)
    single_intent = "user_type" in query
    price_col = pd.to_numeric(df.get("Price (VND)", pd.Series(0, index=df.index)), errors="coerce")
    weight_col = pd.to_numeric(df.get("Weight (kg)", pd.Series(1.8, index=df.index)), errors="coerce")

    # =========================
    # TASK SCORE
    # =========================
    INTENT_SCORE_MAP = {
        "gaming": ["gaming_score"],
        "ai": ["ai_graphics_score"],
        "business": ["office_score"],
        "office": ["office_score"],
        "study": ["office_score", "portability_score"],
        "student": ["office_score", "portability_score"],
        "general": ["general_score"],
    }

    if single_intent:
        ut = user_types[0]
        cols = INTENT_SCORE_MAP.get(ut, ["general_score"])
        cols = [c for c in cols if c in df.columns]
        if not cols:
            cols = ["general_score"] if "general_score" in df.columns else ["office_score"]
            cols = [c for c in cols if c in df.columns]
        
        if cols:
            df["task_score"] = df[cols].mean(axis=1)
        else:
            df["task_score"] = 0.5
    else:
        cols = []
        for ut in user_types:
            cols.extend(INTENT_SCORE_MAP.get(ut, []))
        cols = list(set(cols))
        cols = [c for c in cols if c in df.columns]
        if not cols:
            cols = ["general_score"] if "general_score" in df.columns else ["office_score"]
            cols = [c for c in cols if c in df.columns]
            
        if cols:
            df["task_score"] = df[cols].min(axis=1)
        else:
            df["task_score"] = 0.5

    # =========================
    # PRICE / AFFORDABILITY
    # =========================
    if "price_max" in query:
        budget = query["price_max"]
        ratio = price_col / budget
        ideal = 0.85 if single_intent and user_types[0] == "gaming" else 0.75
        df["price_fit"] = (1 - abs(ratio - ideal)).clip(0, 1)
        df["affordability_score"] = df["price_fit"]
    else:
        if "norm_price" in df.columns:
            df["affordability_score"] = (1 - df["norm_price"]).clip(0, 1)
        else:
            price = price_col
            p10 = price.quantile(0.10)
            p90 = price.quantile(0.90)
            denom = (p90 - p10) if (p90 - p10) != 0 else 1.0
            price_norm = ((price - p10) / denom).clip(0, 1)
            df["affordability_score"] = (1 - price_norm).clip(0, 1)
        df["price_fit"] = 0.5
        
        if query.get("pref_cheap") is True:
            penalty_mask = price_col > 22_000_000
            df.loc[penalty_mask, "affordability_score"] *= 0.5
            heavy_penalty_mask = price_col > 30_000_000
            df.loc[heavy_penalty_mask, "affordability_score"] *= 0.2

    # =========================
    # WEIGHT SCORE
    # =========================
    if "norm_weight" in df.columns:
        # norm_weight is already inverted in feature engineering: higher means lighter.
        df["weight_score"] = df["norm_weight"].clip(0, 1)
    else:
        w = weight_col
        w10 = w.quantile(0.10)
        w90 = w.quantile(0.90)
        denom_w = (w90 - w10) if (w90 - w10) != 0 else 1.0
        w_norm = ((w - w10) / denom_w).clip(0, 1)
        df["weight_score"] = (1 - w_norm).clip(0, 1)

    # =========================
    # FINAL SCORE CALCULATION
    # =========================
    AFFORDABILITY_WEIGHT = {
        "student": 0.30,
        "study": 0.22,
        "business": 0.15,
        "office": 0.15,
        "general": 0.18,
        "ai": 0.10,
        "gaming": 0.05,
    }
    WEIGHT_PREF_WEIGHT = {
        "student": 0.18,
        "study": 0.22,
        "business": 0.15,
        "office": 0.15,
        "general": 0.12,
        "ai": 0.08,
        "gaming": 0.05,
    }

    if single_intent and user_types[0] == "gaming":
        df["final_score"] = df["task_score"]
    else:
        if single_intent:
            base_aff = AFFORDABILITY_WEIGHT.get(user_types[0], 0.18)
            base_wt = WEIGHT_PREF_WEIGHT.get(user_types[0], 0.12)
        else:
            base_aff = max(AFFORDABILITY_WEIGHT.get(ut, 0.18) for ut in user_types)
            base_wt = max(WEIGHT_PREF_WEIGHT.get(ut, 0.12) for ut in user_types)

        if query.get("pref_cheap") is True:
            base_aff = max(base_aff, 0.30)

        if query.get("pref_light") is True:
            base_wt = max(base_wt, 0.22)
        elif query.get("pref_light") is False:
            base_wt = min(base_wt, 0.05)

        w_aff = min(base_aff, 0.40)
        w_wt = min(base_wt, 0.30)
        w_task = max(0.0, 1.0 - w_aff - w_wt)

        df["final_score"] = (
            df["task_score"] * w_task
            + df["affordability_score"] * w_aff
            + df["weight_score"] * w_wt
        )

    # =========================
    # SOFT BONUSES
    # =========================
    bonus = 0.0

    # FPT Shop Installment Bonus
    if query.get("pref_installment") is True:
        for col in ["Is Installment 0%", "is_installment_0", "installment_0"]:
            if col in df.columns:
                has_inst = df[col].fillna("").astype(str).str.lower().isin(["true", "1", "yes", "có"])
                bonus += 0.15 * has_inst.astype(float)
                break

    # FPT Shop Student Discount Bonus
    if query.get("is_student") is True:
        for col in ["Student Discount (VND)", "student_discount"]:
            if col in df.columns:
                has_disc = (pd.to_numeric(df[col], errors="coerce").fillna(0) > 0).astype(float)
                bonus += 0.10 * has_disc
                break

    # FPT Shop Gifts Bonus
    if query.get("need_gifts") is True:
        for col in ["Gifts", "gifts", "quà tặng"]:
            if col in df.columns:
                has_gifts = df[col].fillna("").astype(str).str.strip().ne("").astype(float)
                bonus += 0.08 * has_gifts
                break

    # Brand preference bonus
    brand_pref = query.get("brand_preferences") or {}
    prefer = set(_norm_brand(x) for x in (brand_pref.get("prefer") or []) if x)
    if prefer and "Manufacturer" in df.columns:
        df["_brand_prefer"] = df["Manufacturer"].fillna("").map(_norm_brand).isin(prefer).astype(float)
        bonus += 0.25 * df["_brand_prefer"]

    # Battery requirement bonus
    batt_req = query.get("battery_requirements")
    if isinstance(batt_req, dict) and batt_req.get("min_wh") is not None:
        min_wh = float(batt_req["min_wh"])
        if "Battery (Wh)" in df.columns:
            df["_battery_wh"] = df["Battery (Wh)"]
        elif "Battery" in df.columns:
            df["_battery_wh"] = df["Battery"].apply(_parse_battery_wh)
        else:
            df["_battery_wh"] = 0
        
        df["_battery_ok"] = (df["_battery_wh"].fillna(0) >= min_wh).astype(float)
        bonus += 0.02 * df["_battery_ok"]
    
    if "battery_score" in df.columns:
        bonus += 0.015 * df["battery_score"].fillna(0)

    # Display refresh rate bonus
    disp = query.get("display_requirements")
    if isinstance(disp, dict) and disp.get("min_refresh_hz") is not None and "Refresh Rate (Hz)" in df.columns:
        min_hz = float(disp["min_refresh_hz"])
        df["_hz_ok"] = (df["Refresh Rate (Hz)"].fillna(0) >= min_hz).astype(float)
        bonus += 0.015 * df["_hz_ok"]

    # Ready flags bonuses
    if "is_gaming_ready" in df.columns and any(ut == "gaming" for ut in user_types):
         bonus += 0.015 * df["is_gaming_ready"].astype(float)
    if "is_ai_ready" in df.columns and any(ut == "ai" for ut in user_types):
         bonus += 0.04 * df["is_ai_ready"].astype(float)
    if "is_ultrabook" in df.columns and any(ut in ["business", "office", "student"] for ut in user_types):
         bonus += 0.02 * df["is_ultrabook"].astype(float)

    # =========================
    # ABSOLUTE PENALTIES
    # =========================
    if query.get("pref_light") is True:
        penalty_1_7 = weight_col > 1.7
        df.loc[penalty_1_7, "final_score"] *= 0.85
        
        penalty_2_0 = weight_col > 2.0
        df.loc[penalty_2_0, "final_score"] *= 0.65
        
        penalty_2_3 = weight_col > 2.3
        df.loc[penalty_2_3, "final_score"] *= 0.35
        
        penalty_2_6 = weight_col > 2.6
        df.loc[penalty_2_6, "final_score"] *= 0.08

    # =========================
    # BATTERY PRIORITY
    # =========================
    if query.get("pref_battery") is True:
        if "battery_score" in df.columns:
            bonus += 0.08 * df["battery_score"].fillna(0)
        if "_battery_wh" in df.columns:
            penalty_mask = df["_battery_wh"].fillna(0) < 45
            df.loc[penalty_mask, "final_score"] *= 0.75

    df["final_score"] = (df["final_score"] + bonus).clip(0, 1).round(4)

    return df
