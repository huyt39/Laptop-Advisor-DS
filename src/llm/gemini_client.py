# src/llm/gemini_client.py
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Optional

try:
    from google import genai
    from google.genai import types
except Exception:
    genai = None
    types = None

from src.llm.schemas_v2 import IntentV2
from src.llm.prompts import INTENT_SYSTEM_PROMPT_V2, ADVICE_SYSTEM_PROMPT


def _extract_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty text; cannot parse JSON.")

    try:
        return json.loads(text)
    except Exception:
        pass

    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not m:
        raise ValueError(f"No JSON object found in: {text[:200]}")
    return json.loads(m.group(0))


class GeminiClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_intent: str = "gemini-2.0-flash",
        model_advice: str = "gemini-2.0-flash",
    ) -> None:
        api_key = api_key or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise RuntimeError("Missing GEMINI_API_KEY (or GOOGLE_API_KEY).")
        if genai is None or types is None:
            raise RuntimeError("Missing google-genai package.")

        self.client = genai.Client(api_key=api_key)
        self.model_intent = model_intent
        self.model_advice = model_advice

    def extract_intent(self, user_text: str) -> IntentV2:
        resp = self.client.models.generate_content(
            model=self.model_intent,
            contents=user_text,
            config=types.GenerateContentConfig(
                system_instruction=INTENT_SYSTEM_PROMPT_V2,
                response_mime_type="application/json",
                temperature=0.0,
            ),
        )

        obj = _extract_json(getattr(resp, "text", "") or "")
        return IntentV2(**obj)

    def generate_advice(self, user_text: str, intent: Dict[str, Any], recommendations: Any) -> str:
        payload = {
            "user_text": user_text,
            "intent": intent,
            "recommendations": recommendations,
        }

        resp = self.client.models.generate_content(
            model=self.model_advice,
            contents=json.dumps(payload, ensure_ascii=False),
            config=types.GenerateContentConfig(
                system_instruction=ADVICE_SYSTEM_PROMPT,
                temperature=0.6,
            ),
        )

        text = (getattr(resp, "text", None) or "").strip()
        if not text:
            raise RuntimeError("Gemini returned empty advice.")
        return text

    def generate_advice_stream(self, user_text: str, intent: Dict[str, Any], recommendations: Any):
        payload = {
            "user_text": user_text,
            "intent": intent,
            "recommendations": recommendations,
        }

        return self.client.models.generate_content_stream(
            model=self.model_advice,
            contents=json.dumps(payload, ensure_ascii=False),
            config=types.GenerateContentConfig(
                system_instruction=ADVICE_SYSTEM_PROMPT,
                temperature=0.6,
            ),
        )
