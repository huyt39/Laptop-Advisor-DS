# src/api/main.py
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from src.llm.schemas_v2 import BatteryRequirements, BrandPreferences, DisplayRequirements, IntentV2
from src.llm.gemini_client import GeminiClient
from src.llm.prompts import fallback_advice_no_llm
from src.advisor.features import prepare_laptop_dataframe
from src.advisor.recommend_service import build_query_from_intent, recommendations_to_json
from src.advisor.advisor import recommend_laptops
from fastapi.middleware.cors import CORSMiddleware



# Bootstrap

load_dotenv()

DEFAULT_DATA_CANDIDATES = [
    "data/fpt_laptops_features.csv",
]
DATA_PATH = os.getenv("LAPTOPS_CSV")
if not DATA_PATH:
    DATA_PATH = next((p for p in DEFAULT_DATA_CANDIDATES if os.path.exists(p)), DEFAULT_DATA_CANDIDATES[0])
FPT_ONLY = os.getenv("FPT_ONLY", "true").strip().lower() not in {"0", "false", "no"}

app = FastAPI(title="FPT Shop Laptop Advisor API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



# Load dataset once

try:
    df = pd.read_csv(DATA_PATH)
except FileNotFoundError as e:
    # Fallback to an empty dataframe to let the server start
    print(f"Dataset not found at: {DATA_PATH}. Creating empty dataframe.")
    df = pd.DataFrame()

if not df.empty:
    df = prepare_laptop_dataframe(df, fpt_only=FPT_ONLY)

if "RAM (GB)" in df.columns and not df.empty:
    df = df[df["RAM (GB)"].between(4, 128) | df["RAM (GB)"].isna()]

if "Price (VND)" in df.columns and not df.empty:
    df = df[df["Price (VND)"].isna() | df["Price (VND)"].between(3_000_000, 300_000_000)]


# Gemini client (allow running without GEMINI_API_KEY)

try:
    gemini = GeminiClient(
        api_key=os.getenv("GEMINI_API_KEY"),
        model_intent=os.getenv("GEMINI_MODEL_INTENT", "gemini-2.0-flash"),
        model_advice=os.getenv("GEMINI_MODEL_ADVICE", "gemini-2.0-flash"),
    )
    USE_LLM = True
except Exception:
    gemini = None
    USE_LLM = False


# API models

class ChatRequest(BaseModel):
    text: str = Field(..., min_length=1)


class ChatResponse(BaseModel):
    intent: Dict[str, Any]
    query: Dict[str, Any]
    recommendations: List[Dict[str, Any]]
    answer: str


# Rule-based patches (robustness)

_GAMING_KW = [
    "chơi game", "gaming", "fps", "valorant", "cs2", "counter strike", "pubg",
    "lol", "liên minh", "dota", "gta", "elden ring", "aaa", "game"
]

_CHEAP_KW = ["rẻ", "tiết kiệm", "giá tốt", "giá mềm", "giá hợp lý", "ngon bổ rẻ"]
_LIGHT_KW = ["nhẹ", "mỏng nhẹ", "dễ mang", "di chuyển", "portable"]
_BATTERY_KW = ["pin", "pin trâu", "pin lâu", "dung lượng pin", "battery"]
_BRANDS = ["asus", "acer", "dell", "hp", "lenovo", "msi", "apple", "macbook", "lg", "samsung", "gigabyte"]


def _extract_budget_vnd(text: str) -> Optional[int]:
    t = text.lower().strip()

    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(triệu|tr)\b", t)
    if m:
        val = float(m.group(1).replace(",", "."))
        return int(val * 1_000_000)

    m2 = re.search(r"\b(\d{7,10})\b", t)
    if m2:
        v = int(m2.group(1))
        if 5_000_000 <= v <= 200_000_000:
            return v

    return None


def _extract_price_range_vnd(text: str) -> tuple[Optional[int], Optional[int]]:
    t = text.lower().strip()
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:-|đến|toi|tới|den)\s*(\d+(?:[.,]\d+)?)\s*(triệu|tr)\b", t)
    if not m:
        return None, None
    low = float(m.group(1).replace(",", "."))
    high = float(m.group(2).replace(",", "."))
    if low > high:
        low, high = high, low
    return int(low * 1_000_000), int(high * 1_000_000)


def _extract_ram_gb(text: str) -> Optional[int]:
    t = text.lower()
    for m in re.finditer(r"(?:ram|bộ nhớ)\s*(?:tối thiểu|ít nhất|>=|từ)?\s*(\d{1,3})\s*gb", t):
        n = int(m.group(1))
        if 4 <= n <= 64:
            return n
    return None


def _extract_storage_gb(text: str) -> Optional[int]:
    t = text.lower()
    m = re.search(r"(?:ssd|ổ cứng|lưu trữ|storage)\s*(?:tối thiểu|ít nhất|>=|từ)?\s*(\d+(?:[.,]\d+)?)\s*tb", t)
    if m:
        return int(float(m.group(1).replace(",", ".")) * 1000)
    m = re.search(r"(?:ssd|ổ cứng|lưu trữ|storage)\s*(?:tối thiểu|ít nhất|>=|từ)?\s*(\d{3,4})\s*gb", t)
    if m:
        return int(m.group(1))
    return None


def _extract_screen_size_inch(text: str) -> Optional[float]:
    t = text.lower()
    m = re.search(r"(\d{2}(?:[.,]\d)?)\s*(?:inch|inches|\")", t)
    if not m:
        return None
    value = float(m.group(1).replace(",", "."))
    if 10 <= value <= 18:
        return value
    return None


def _extract_max_weight_kg(text: str) -> Optional[float]:
    t = text.lower()
    m = re.search(r"(?:dưới|không quá|tối đa|<=|<)\s*(\d(?:[.,]\d{1,2})?)\s*kg", t)
    if not m:
        return None
    value = float(m.group(1).replace(",", "."))
    if 0.5 <= value <= 5:
        return value
    return None


def _extract_min_battery_wh(text: str) -> Optional[float]:
    t = text.lower()
    m = re.search(r"(?:pin|battery)?\s*(?:tối thiểu|ít nhất|>=|từ)?\s*(\d{2,3})\s*wh", t)
    if not m:
        return None
    value = float(m.group(1))
    if 20 <= value <= 120:
        return value
    return None


def _extract_brand_preferences(text: str) -> tuple[list[str], list[str]]:
    t = text.lower()
    preferred = []
    excluded = []
    for brand in _BRANDS:
        normalized = "apple" if brand == "macbook" else brand
        brand_pattern = rf"\b{re.escape(brand)}\b"
        if not re.search(brand_pattern, t):
            continue
        exclusion_pattern = (
            rf"(?:không\s+(?:thích|muốn|chọn|lấy)|tránh|loại|trừ)"
            rf"(?:\s+\w+){{0,3}}\s+{brand_pattern}"
        )
        if re.search(exclusion_pattern, t):
            excluded.append(normalized)
        else:
            preferred.append(normalized)
    return list(dict.fromkeys(preferred)), list(dict.fromkeys(excluded))


def _extract_top_n(text: str) -> Optional[int]:
    t = text.lower()

    patterns = [
        r"\btop\s*(\d{1,2})\b",
        r"\bgợi\s*ý\s*(\d{1,2})\s*(máy|lựa\s*chọn|option)?\b",
        r"\bcho\s*tôi\s*(\d{1,2})\s*(máy|lựa\s*chọn|option)\b",
        r"\bđề\s*xuất\s*(\d{1,2})\s*(máy|lựa\s*chọn|option)?\b",
        r"\btôi\s*muốn\s*(\d{1,2})\s*(máy|lựa\s*chọn|option)?\b",
        r"\btôi\s*cần\s*(\d{1,2})\s*(máy|lựa\s*chọn|option)?\b",
    ]
    for p in patterns:
        m = re.search(p, t)
        if m:
            n = int(m.group(1))
            if 1 <= n <= 10:
                return n
    return None

STUDENT_PRICE_CAP_VND = 20_000_000


def _extract_cpu_gen(text: str) -> Optional[int]:
    t = text.lower()
    patterns = [
        r"(?:đời|thế hệ|gen)\s*(\d{1,2})\b",
        r"(\d{1,2})(?:th|nd|rd|st)?\s*gen\b",
    ]
    for p in patterns:
        m = re.search(p, t)
        if m:
            return int(m.group(1))
    return None


def _extract_cpu_brand(text: str) -> Optional[str]:
    t = text.lower()
    m = re.search(r"\b(i[3579])\b", t)
    if m:
        return m.group(1)
    
    m2 = re.search(r"\b(ryzen|r)\s*([3579])\b", t)
    if m2:
        return f"ryzen {m2.group(2)}"
    
    return None


def _extract_cpu_manu(text: str) -> Optional[str]:
    t = text.lower()
    if "intel" in t:
        return "Intel"
    if "amd" in t:
        return "AMD"
    return None


def patch_intent_from_text(user_text: str, intent: IntentV2) -> IntentV2:
    t = user_text.lower()

    # 1) Detect intents from text
    detected = detect_user_types_from_text(t)

    # 2) Merge with Gemini result
    gemini_types = []
    if intent.user_types:
        gemini_types = [u.lower() for u in intent.user_types]
    elif intent.user_type:
        gemini_types = [intent.user_type.lower()]

    merged = list(dict.fromkeys(gemini_types + detected))
    if len(merged) > 1 and "general" in merged:
        merged = [x for x in merged if x != "general"]

    # 3) Decide single vs multi
    if len(merged) >= 2:
        intent.user_types = merged
        intent.user_type = None
    elif len(merged) == 1:
        intent.user_type = merged[0]
        intent.user_types = None
    else:
        if not intent.user_type and not intent.user_types:
            intent.user_type = "general"
            intent.user_types = None

    # 4) Budget extraction from text
    price_min, price_max = _extract_price_range_vnd(user_text)
    if price_min is not None:
        intent.price_min = price_min
    if price_max is not None:
        intent.price_max = price_max

    budget = _extract_budget_vnd(user_text)
    if budget is not None:
        if any(k in t for k in ["dưới", "<=", "under", "tối đa", "max", "không quá"]):
            intent.price_max = budget
        else:
            if intent.price_max is None:
                intent.price_max = budget

    ram = _extract_ram_gb(user_text)
    if ram is not None:
        intent.min_ram_gb = ram

    storage = _extract_storage_gb(user_text)
    if storage is not None:
        intent.min_storage_gb = storage

    screen_size = _extract_screen_size_inch(user_text)
    if screen_size is not None:
        intent.display_requirements = DisplayRequirements(
            screen_size_inch=screen_size,
            screen_size_tolerance=0.25,
        )

    max_weight = _extract_max_weight_kg(user_text)
    if max_weight is not None:
        intent.max_weight_kg = max_weight
        intent.pref_light = True

    min_battery_wh = _extract_min_battery_wh(user_text)
    if min_battery_wh is not None:
        intent.battery_requirements = BatteryRequirements(min_wh=min_battery_wh)
        intent.pref_battery = True

    preferred_brands, excluded_brands = _extract_brand_preferences(user_text)
    current_brand_pref = intent.brand_preferences
    current_prefer = list(current_brand_pref.prefer or []) if current_brand_pref else []
    current_exclude = list(current_brand_pref.exclude or []) if current_brand_pref else []
    merged_prefer = list(dict.fromkeys(current_prefer + preferred_brands))
    merged_exclude = list(dict.fromkeys(current_exclude + excluded_brands))
    merged_prefer = [brand for brand in merged_prefer if brand not in merged_exclude]
    if merged_prefer or merged_exclude:
        intent.brand_preferences = BrandPreferences(
            prefer=merged_prefer or None,
            exclude=merged_exclude or None,
        )

    # 5) CPU Generation extraction
    cpu_gen = _extract_cpu_gen(user_text)
    if cpu_gen is not None:
        intent.min_cpu_gen = cpu_gen

    # 6) CPU Brand extraction
    cpu_brand = _extract_cpu_brand(user_text)
    if cpu_brand is not None:
        intent.cpu_brand = cpu_brand

    # 7) CPU Manufacturer extraction
    cpu_manu = _extract_cpu_manu(user_text)
    if cpu_manu is not None:
        intent.cpu_manufacturer = cpu_manu

    # 8) Preferences from keywords
    if any(k in t for k in _CHEAP_KW):
        intent.pref_cheap = True
    if any(k in t for k in _LIGHT_KW):
        intent.pref_light = True
    if any(k in t for k in _BATTERY_KW):
        intent.pref_battery = True

    # 9) Student semantics
    is_student_persona = (
        (intent.user_type == "student")
        or (intent.user_types is not None and "student" in intent.user_types)
    )
    if is_student_persona:
        intent.pref_cheap = True
        intent.is_student = True
        if intent.price_max is None and intent.price_min is None and budget is None:
            intent.price_max = STUDENT_PRICE_CAP_VND

    # 10) FPT retail flags (Installment & Gifts)
    if any(k in t for k in ["trả góp", "góp tháng", "góp 0%"]):
        intent.pref_installment = True
    if any(k in t for k in ["quà", "quà tặng", "khuyến mại", "khuyến mãi", "tặng kèm"]):
        intent.need_gifts = True

    # 11) top_n override
    n = _extract_top_n(user_text)
    if n is not None:
        intent.top_n = n

    return intent


INTENT_KW = {
    "gaming": ["chơi game", "gaming", "fps", "valorant", "cs2", "pubg", "lol", "liên minh", "game"],
    "ai": ["ai", "học máy", "machine learning", "deep learning", "cuda", "graphics", "nhân tạo"],
    "business": ["doanh nhân", "kinh doanh", "gặp khách"],
    "office": ["văn phòng", "office", "word", "excel", "powerpoint"],
    "study": ["học tập", "đi học", "làm bài", "zoom", "meet", "lập trình", "code"],
    "student": ["học sinh", "hs", "sinh viên", "sv"],
}


def detect_user_types_from_text(t: str) -> list[str]:
    t = t.lower()
    found = []
    for ut, kws in INTENT_KW.items():
        if any(k in t for k in kws):
            found.append(ut)
    return list(dict.fromkeys(found))


def sort_recommendations_for_intent(intent: Dict[str, Any], recs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(recs, key=lambda x: float(x.get("scores", {}).get("final_score") or 0), reverse=True)


# Endpoints

@app.get("/health")
def health():
    return {
        "status": "ok",
        "rows": int(len(df)) if not df.empty else 0,
        "use_llm": USE_LLM,
        "data_path": DATA_PATH,
    }


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    user_text = req.text.strip()
    if not user_text:
        raise HTTPException(status_code=400, detail="text must not be empty")

    if df.empty:
        raise HTTPException(status_code=500, detail="Database CSV is empty or not loaded.")

    # 1) Extract intent
    if USE_LLM:
        try:
            intent_obj = gemini.extract_intent(user_text)
        except Exception:
            intent_obj = IntentV2(user_type="general")
    else:
        intent_obj = IntentV2(user_type="general")

    # 2) Patch intent from raw text for robustness
    intent_obj = patch_intent_from_text(user_text, intent_obj)
    intent_dict = intent_obj.model_dump(exclude_none=True)

    # 3) Build query for the recommend engine
    query = build_query_from_intent(intent_obj)
    if "user_type" not in query and "user_types" not in query:
        query["user_type"] = "general"

    # 4) Recommend
    try:
        df_top = recommend_laptops(df, query, top_n=intent_obj.top_n)
        recs = recommendations_to_json(df_top, query)
        recs = sort_recommendations_for_intent(intent_dict, recs)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Recommendation engine error: {e}")

    # 5) Generate advice text
    if USE_LLM:
        try:
            answer = gemini.generate_advice(
                user_text=user_text,
                intent=intent_dict,
                recommendations=recs,
            )
            if not answer or not answer.strip():
                answer = fallback_advice_no_llm(user_text, intent_dict, recs)
        except Exception:
            answer = fallback_advice_no_llm(user_text, intent_dict, recs)
    else:
        answer = fallback_advice_no_llm(user_text, intent_dict, recs)
    return {
        "intent": intent_dict,
        "query": query,
        "recommendations": recs,
        "answer": answer,
    }


@app.post("/chat/stream")
def chat_stream(req: ChatRequest):
    user_text = req.text.strip()
    if not user_text:
        raise HTTPException(status_code=400, detail="text must not be empty")

    if df.empty:
        raise HTTPException(status_code=500, detail="Database CSV is empty or not loaded.")

    # 1) Extract intent
    if USE_LLM:
        try:
            intent_obj = gemini.extract_intent(user_text)
        except Exception:
            intent_obj = IntentV2(user_type="general")
    else:
        intent_obj = IntentV2(user_type="general")

    # 2) Patch intent from raw text for robustness
    intent_obj = patch_intent_from_text(user_text, intent_obj)
    intent_dict = intent_obj.model_dump(exclude_none=True)

    # 3) Build query for the recommend engine
    query = build_query_from_intent(intent_obj)
    if "user_type" not in query and "user_types" not in query:
        query["user_type"] = "general"

    # 4) Recommend
    try:
        df_top = recommend_laptops(df, query, top_n=intent_obj.top_n)
        recs = recommendations_to_json(df_top, query)
        recs = sort_recommendations_for_intent(intent_dict, recs)
    except Exception as e:
        recs = []

    # 5) Stream advice
    def event_generator():
        # First send the metadata (intent, query, recommendations)
        metadata = {
            "type": "metadata",
            "intent": intent_dict,
            "query": query,
            "recommendations": recs
        }
        yield json.dumps(metadata, ensure_ascii=False) + "\n"

        if USE_LLM:
            try:
                response_stream = gemini.generate_advice_stream(
                    user_text=user_text,
                    intent=intent_dict,
                    recommendations=recs
                )
                for chunk in response_stream:
                    if chunk.text:
                        yield json.dumps({"type": "text", "content": chunk.text}, ensure_ascii=False) + "\n"
            except Exception:
                fallback_txt = fallback_advice_no_llm(user_text, intent_dict, recs)
                yield json.dumps({"type": "text", "content": fallback_txt}, ensure_ascii=False) + "\n"
        else:
            fallback_txt = fallback_advice_no_llm(user_text, intent_dict, recs)
            yield json.dumps({"type": "text", "content": fallback_txt}, ensure_ascii=False) + "\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")
