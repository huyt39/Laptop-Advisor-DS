from __future__ import annotations

import json
import math
import os
import re
import time
from typing import Any, Dict, List, Optional

try:
    from google import genai
    from google.genai import types
except Exception:
    genai = None
    types = None


def precision_at_k(relevances: List[int], k: int) -> float:
    if k <= 0:
        return 0.0
    denom = min(k, len(relevances)) or k
    return sum(1 for r in relevances[:k] if r > 0) / denom


def strict_precision_at_k(relevances: List[int], k: int) -> float:
    if k <= 0:
        return 0.0
    denom = min(k, len(relevances)) or k
    return sum(1 for r in relevances[:k] if r == 2) / denom


def normalized_relevance_at_k(relevances: List[int], k: int) -> float:
    if k <= 0:
        return 0.0
    trimmed = relevances[:k]
    denom = (min(k, len(trimmed)) or k) * 2
    return sum(trimmed) / denom if denom else 0.0


def dcg_at_k(relevances: List[int], k: int) -> float:
    return sum((2 ** rel - 1) / math.log2(i + 2) for i, rel in enumerate(relevances[:k]))


def ndcg_at_k(relevances: List[int], k: int) -> float:
    dcg = dcg_at_k(relevances, k)
    ideal = dcg_at_k(sorted(relevances, reverse=True), k)
    return dcg / ideal if ideal > 0 else 0.0


def mrr(relevances_list: List[List[int]]) -> float:
    if not relevances_list:
        return 0.0
    total = 0.0
    for relevances in relevances_list:
        for i, rel in enumerate(relevances):
            if rel > 0:
                total += 1.0 / (i + 1)
                break
    return total / len(relevances_list)


def _num(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "có", "co", "in stock", "còn hàng", "con hang"}


def _contains_any(text: Any, needles: List[str]) -> bool:
    haystack = str(text or "").lower()
    return any(n in haystack for n in needles)


def _norm_brand(value: Any) -> str:
    text = str(value or "").strip().lower()
    for brand in ["asus", "acer", "dell", "hp", "lenovo", "msi", "apple", "macbook", "lg", "samsung", "gigabyte"]:
        if brand in text:
            return "apple" if brand == "macbook" else brand
    return text


def _intel_gen_from_cpu_number(value: Any) -> Optional[int]:
    n = _num(value)
    if n is None:
        return None
    n = int(n)
    if 1 <= n <= 99:
        return n
    if 100 <= n < 1000:
        return 14
    text = str(n)
    if len(text) >= 4:
        return int(text[:2])
    return None


def check_constraint_satisfaction(laptop: Dict[str, Any], constraints: Dict[str, Any]) -> Dict[str, Any]:
    details: Dict[str, Dict[str, Any]] = {}
    all_ok = True

    def add(name: str, required: Any, actual: Any, ok: bool) -> None:
        nonlocal all_ok
        details[name] = {"required": required, "actual": actual, "ok": bool(ok)}
        all_ok = all_ok and bool(ok)

    price = _num(laptop.get("price_vnd"))
    if "price_min" in constraints:
        add("price_min", constraints["price_min"], price, price is not None and price >= constraints["price_min"])
    if "price_max" in constraints:
        add("price_max", constraints["price_max"], price, price is not None and price <= constraints["price_max"])

    for src_key, dst_key in [
        ("min_ram_gb", "ram_gb"),
        ("min_storage_gb", "storage_gb"),
        ("max_weight_kg", "weight_kg"),
        ("min_refresh_hz", "refresh_hz"),
    ]:
        if src_key not in constraints:
            continue
        actual = _num(laptop.get(dst_key))
        required = constraints[src_key]
        ok = actual is not None and (actual <= required if src_key.startswith("max_") else actual >= required)
        add(src_key, required, actual, ok)

    flags = laptop.get("flags", {}) or {}
    for flag in ["is_light", "is_gaming_ready", "is_ai_ready", "is_ultrabook", "is_business_ready"]:
        if flag in constraints:
            actual = bool(flags.get(flag))
            add(flag, constraints[flag], actual, actual == bool(constraints[flag]))

    if "brand_in" in constraints:
        allowed = {str(x).lower() for x in constraints["brand_in"]}
        actual = _norm_brand(laptop.get("brand"))
        add("brand_in", sorted(allowed), actual, actual in allowed)

    if "brand_not_in" in constraints:
        blocked = {str(x).lower() for x in constraints["brand_not_in"]}
        actual = _norm_brand(laptop.get("brand"))
        add("brand_not_in", sorted(blocked), actual, actual not in blocked)

    if "screen_size_inch" in constraints:
        actual = _num(laptop.get("screen_inch"))
        target = float(constraints["screen_size_inch"])
        tolerance = float(constraints.get("screen_size_tolerance", 0.25))
        ok = actual is not None and (target - tolerance) <= actual <= (target + tolerance)
        add("screen_size_inch", target, actual, ok)

    if "min_battery_wh" in constraints:
        actual = _num(laptop.get("battery_wh"))
        required = float(constraints["min_battery_wh"])
        add("min_battery_wh", required, actual, actual is not None and actual >= required)

    if "min_cpu_gen" in constraints:
        actual = _intel_gen_from_cpu_number(laptop.get("cpu_generation"))
        required = int(constraints["min_cpu_gen"])
        manu = str(laptop.get("cpu_manufacturer") or "").lower()
        ok = actual is not None and actual >= required if "intel" in manu else True
        add("min_cpu_gen", required, actual, ok)

    if "is_installment_0" in constraints:
        actual = bool(laptop.get("is_installment_0"))
        add("is_installment_0", constraints["is_installment_0"], actual, actual == bool(constraints["is_installment_0"]))

    if "has_gifts" in constraints:
        actual = bool(str(laptop.get("gifts") or "").strip())
        add("has_gifts", constraints["has_gifts"], actual, actual == bool(constraints["has_gifts"]))

    if "stock_status" in constraints:
        actual = str(laptop.get("stock_status") or "").strip().lower()
        required = str(constraints["stock_status"]).strip().lower()
        add("stock_status", required, actual, actual == required or _truthy(actual) == _truthy(required))

    return {"satisfied": all_ok, "details": details}


def constraint_satisfaction_rate(recommendations: List[Dict[str, Any]], constraints: Dict[str, Any]) -> float:
    if not recommendations:
        return 0.0
    return sum(1 for r in recommendations if check_constraint_satisfaction(r, constraints)["satisfied"]) / len(recommendations)


def unique_name_rate(recommendations: List[Dict[str, Any]]) -> float:
    if not recommendations:
        return 0.0
    names = [str(r.get("name") or "").strip().lower() for r in recommendations]
    return len(set(names)) / len(names)


class RuleBasedJudge:
    """Deterministic relevance judge for offline benchmark runs."""

    def judge(self, query: str, laptop: Dict[str, Any], constraints: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        constraints = constraints or {}
        checks = check_constraint_satisfaction(laptop, constraints)
        score = 0.0
        reasons: List[str] = []

        if checks["details"]:
            ok_count = sum(1 for d in checks["details"].values() if d["ok"])
            score += ok_count / len(checks["details"]) * 1.2
            if ok_count == len(checks["details"]):
                reasons.append("đạt các ràng buộc chính")
            else:
                reasons.append(f"đạt {ok_count}/{len(checks['details'])} ràng buộc")

        name = laptop.get("name") or ""
        flags = laptop.get("flags", {}) or {}
        q = query.lower()

        if _contains_any(q, ["gaming", "chơi game", "game"]):
            if flags.get("is_gaming_ready") or _contains_any(name, ["gaming", "tuf", "rog", "nitro", "loq", "legion", "victus"]):
                score += 0.5
                reasons.append("có tín hiệu phù hợp gaming")
        if _contains_any(q, ["văn phòng", "office", "học", "sinh viên", "hssv"]):
            if flags.get("is_business_ready") or flags.get("is_ultrabook") or _num(laptop.get("ram_gb") or 0) >= 8:
                score += 0.4
                reasons.append("phù hợp văn phòng/học tập")
        if _contains_any(q, ["doanh nhân", "kinh doanh", "gặp khách"]):
            if flags.get("is_business_ready") or _norm_brand(laptop.get("brand")) == "apple":
                score += 0.35
                reasons.append("phù hợp nhu cầu doanh nhân")
        if _contains_any(q, ["ai", "học máy", "machine learning", "deep learning", "lập trình", "code"]):
            if flags.get("is_ai_ready") or _contains_any(laptop.get("gpu_model"), ["rtx", "arc", "apple m"]):
                score += 0.45
                reasons.append("có tín hiệu phù hợp AI/lập trình")
        if _contains_any(q, ["nhẹ", "mỏng nhẹ", "di chuyển"]):
            if flags.get("is_light") or (_num(laptop.get("weight_kg")) is not None and _num(laptop.get("weight_kg")) <= 1.7):
                score += 0.3
                reasons.append("đáp ứng nhu cầu mỏng nhẹ")
        if _contains_any(q, ["pin", "pin trâu", "pin lâu", "battery"]):
            battery_life = _num(laptop.get("battery_life_hours"))
            battery_wh = _num(laptop.get("battery_wh"))
            if (battery_life is not None and battery_life >= 12) or (battery_wh is not None and battery_wh >= 60):
                score += 0.35
                reasons.append("có tín hiệu pin tốt")
        if _contains_any(q, ["macbook", "apple"]):
            if _norm_brand(laptop.get("brand")) == "apple" or _contains_any(name, ["macbook"]):
                score += 0.35
                reasons.append("đúng nhóm MacBook/Apple")
        if _contains_any(q, ["giá hợp lý", "giá tốt", "rẻ", "tiết kiệm"]):
            price = _num(laptop.get("price_vnd"))
            if price is not None and price <= 22_000_000:
                score += 0.25
                reasons.append("mức giá hợp lý")
        if _contains_any(q, ["trả góp", "góp 0%"]) and laptop.get("is_installment_0"):
            score += 0.25
            reasons.append("có hỗ trợ trả góp 0%")
        if _contains_any(q, ["quà", "khuyến mãi", "khuyến mại"]) and laptop.get("gifts"):
            score += 0.2
            reasons.append("có ưu đãi/quà tặng")

        if checks["details"]:
            ok_count = sum(1 for d in checks["details"].values() if d["ok"])
            if ok_count == len(checks["details"]) and ok_count >= 3:
                score += 0.2
                reasons.append("thỏa nhiều ràng buộc cứng")

        relevance = 2 if score >= 1.35 else (1 if score >= 0.55 else 0)
        return {
            "relevance_score": relevance,
            "short_reason": "; ".join(reasons) if reasons else "chưa thấy tín hiệu phù hợp rõ ràng",
            "laptop_name": laptop.get("name", "Unknown"),
            "judge": "rule",
        }

    def judge_batch(self, query: str, laptops: List[Dict[str, Any]], constraints: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        return [self.judge(query, laptop, constraints) for laptop in laptops]


class GeminiJudge:
    RELEVANCE_PROMPT = """You are evaluating FPT Shop laptop recommendations.

User query:
"{query}"

Expected constraints:
{constraints_json}

Laptop:
- Name: {name}
- Brand: {brand}
- Price: {price_vnd} VND
- RAM: {ram_gb} GB
- Storage: {storage_gb} GB
- Weight: {weight_kg} kg
- Screen: {screen_inch} inch
- Installment 0%: {is_installment_0}
- Gifts: {gifts}
- Stock status: {stock_status}
- Flags: {flags_json}

Rubric:
2 = fully relevant, matches the user's use case and key constraints.
1 = partially relevant, reasonable alternative but misses a soft preference or one minor detail.
0 = not relevant, violates an important need or is unsuitable.

Return JSON only:
{{"relevance_score": 0 | 1 | 2, "short_reason": "Vietnamese reason"}}"""

    def __init__(self, api_key: Optional[str] = None, model: str = "gemini-2.0-flash", sleep_between_calls: float = 1.5):
        if genai is None or types is None:
            raise RuntimeError("google-genai package is not available.")
        api_key = api_key or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise RuntimeError("Missing GEMINI_API_KEY or GOOGLE_API_KEY.")
        self.client = genai.Client(api_key=api_key)
        self.model = model
        self.sleep_between_calls = sleep_between_calls

    def judge(self, query: str, laptop: Dict[str, Any], constraints: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        prompt = self.RELEVANCE_PROMPT.format(
            query=query,
            constraints_json=json.dumps(constraints or {}, ensure_ascii=False),
            name=laptop.get("name", "Unknown"),
            brand=laptop.get("brand", "Unknown"),
            price_vnd=laptop.get("price_vnd", "N/A"),
            ram_gb=laptop.get("ram_gb", "N/A"),
            storage_gb=laptop.get("storage_gb", "N/A"),
            weight_kg=laptop.get("weight_kg", "N/A"),
            screen_inch=laptop.get("screen_inch", "N/A"),
            is_installment_0=laptop.get("is_installment_0", False),
            gifts=laptop.get("gifts") or "",
            stock_status=laptop.get("stock_status") or "",
            flags_json=json.dumps(laptop.get("flags", {}), ensure_ascii=False),
        )
        resp = self.client.models.generate_content(
            model=self.model,
            contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json", temperature=0.0),
        )
        text = getattr(resp, "text", "") or ""
        try:
            obj = json.loads(text)
        except Exception:
            match = re.search(r"\{.*\}", text, flags=re.DOTALL)
            obj = json.loads(match.group(0)) if match else {}
        return {
            "relevance_score": int(obj.get("relevance_score", 0)),
            "short_reason": obj.get("short_reason", ""),
            "laptop_name": laptop.get("name", "Unknown"),
            "judge": "gemini",
        }

    def judge_batch(self, query: str, laptops: List[Dict[str, Any]], constraints: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        out = []
        for i, laptop in enumerate(laptops):
            out.append(self.judge(query, laptop, constraints))
            if i < len(laptops) - 1:
                time.sleep(self.sleep_between_calls)
        return out


class RecommendationEvaluator:
    def __init__(
        self,
        top_k: int = 3,
        judge_mode: str = "rule",
        api_key: Optional[str] = None,
        model: str = "gemini-2.0-flash",
        sleep_between_calls: float = 1.5,
    ):
        self.top_k = top_k
        if judge_mode == "gemini":
            self.judge = GeminiJudge(api_key=api_key, model=model, sleep_between_calls=sleep_between_calls)
        else:
            self.judge = RuleBasedJudge()

    def evaluate_single_query(
        self,
        query: str,
        recommendations: List[Dict[str, Any]],
        expected_constraints: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        top_recs = recommendations[: self.top_k]
        judgments = self.judge.judge_batch(query, top_recs, expected_constraints)
        relevances = [int(j.get("relevance_score", 0)) for j in judgments]
        result: Dict[str, Any] = {
            "query": query,
            "num_recommendations": len(recommendations),
            "top_k": self.top_k,
            "precision_at_k": precision_at_k(relevances, self.top_k),
            "strict_precision_at_k": strict_precision_at_k(relevances, self.top_k),
            "normalized_relevance_at_k": normalized_relevance_at_k(relevances, self.top_k),
            "ndcg_at_k": ndcg_at_k(relevances, self.top_k),
            "unique_name_rate": unique_name_rate(top_recs),
            "relevances": relevances,
            "judgments": judgments,
        }
        if expected_constraints is not None:
            result["csr"] = constraint_satisfaction_rate(top_recs, expected_constraints)
            result["constraint_details"] = [check_constraint_satisfaction(r, expected_constraints) for r in top_recs]
        return result

    def evaluate_batch(self, test_cases: List[Dict[str, Any]]) -> Dict[str, Any]:
        results = [
            self.evaluate_single_query(
                query=case["query"],
                recommendations=case.get("recommendations", []),
                expected_constraints=case.get("expected_constraints"),
            )
            for case in test_cases
        ]
        all_relevances = [r["relevances"] for r in results]
        csr_values = [r["csr"] for r in results if "csr" in r]
        full_match_results = [
            r
            for r in results
            if r["strict_precision_at_k"] == 1.0 and ("csr" not in r or r["csr"] == 1.0)
        ]
        return {
            "num_queries": len(results),
            "top_k": self.top_k,
            "aggregate": {
                "avg_precision_at_k": sum(r["precision_at_k"] for r in results) / len(results) if results else 0.0,
                "avg_strict_precision_at_k": sum(r["strict_precision_at_k"] for r in results) / len(results) if results else 0.0,
                "avg_normalized_relevance_at_k": sum(r["normalized_relevance_at_k"] for r in results) / len(results) if results else 0.0,
                "full_match_query_rate": len(full_match_results) / len(results) if results else 0.0,
                "avg_ndcg_at_k": sum(r["ndcg_at_k"] for r in results) / len(results) if results else 0.0,
                "avg_csr": sum(csr_values) / len(csr_values) if csr_values else None,
                "avg_unique_name_rate": sum(r["unique_name_rate"] for r in results) / len(results) if results else 0.0,
                "mrr": mrr(all_relevances),
            },
            "per_query_results": results,
        }
