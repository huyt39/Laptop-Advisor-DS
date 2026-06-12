import json
import unittest

import pandas as pd
from fastapi.testclient import TestClient

from src.advisor.filters import apply_filters
from src.advisor.scorer import apply_scoring
from src.api import main as api_main
from src.llm.schemas_v2 import IntentV2


class RecommendationLogicTests(unittest.TestCase):
    def test_weight_score_rewards_lighter_laptop(self):
        frame = pd.DataFrame(
            [
                {
                    "Product Name": "Light laptop",
                    "Price (VND)": 20_000_000,
                    "Weight (kg)": 1.2,
                    "norm_weight": 1.0,
                    "norm_price": 0.5,
                    "office_score": 0.6,
                    "general_score": 0.6,
                    "battery_score": 0.5,
                    "is_ultrabook": False,
                },
                {
                    "Product Name": "Heavy laptop",
                    "Price (VND)": 20_000_000,
                    "Weight (kg)": 2.5,
                    "norm_weight": 0.0,
                    "norm_price": 0.5,
                    "office_score": 0.6,
                    "general_score": 0.6,
                    "battery_score": 0.5,
                    "is_ultrabook": False,
                },
            ]
        )

        scored = apply_scoring(
            frame,
            {"user_type": "office", "pref_light": True, "price_max": 25_000_000},
        )

        self.assertGreater(scored.loc[0, "weight_score"], scored.loc[1, "weight_score"])
        self.assertGreater(scored.loc[0, "final_score"], scored.loc[1, "final_score"])

    def test_normalized_intel_generation_is_filtered_correctly(self):
        frame = pd.DataFrame(
            [
                {
                    "Product Name": "Intel gen 14",
                    "CPU manufacturer": "Intel",
                    "CPU generation": 14,
                },
                {
                    "Product Name": "Intel gen 12",
                    "CPU manufacturer": "Intel",
                    "CPU generation": 12,
                },
            ]
        )

        filtered = apply_filters(frame, {"user_type": "office", "min_cpu_gen": 13})

        self.assertEqual(filtered["Product Name"].tolist(), ["Intel gen 14"])

    def test_brand_exclusion_is_preserved_from_user_text(self):
        intent = api_main.patch_intent_from_text(
            "Mình không thích Asus, ưu tiên Dell cho văn phòng",
            IntentV2(user_type="general"),
        )

        self.assertEqual(intent.brand_preferences.exclude, ["asus"])
        self.assertEqual(intent.brand_preferences.prefer, ["dell"])


class ChatApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.previous_use_llm = api_main.USE_LLM
        cls.previous_gemini = api_main.gemini
        api_main.USE_LLM = False
        api_main.gemini = None
        cls.client = TestClient(api_main.app)

    @classmethod
    def tearDownClass(cls):
        api_main.USE_LLM = cls.previous_use_llm
        api_main.gemini = cls.previous_gemini

    def test_chat_fallback_returns_ranked_top_k_with_explanations(self):
        response = self.client.post(
            "/chat",
            json={"text": "Gợi ý top 2 laptop gaming dưới 40 triệu"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["intent"]["top_n"], 2)
        self.assertLessEqual(len(payload["recommendations"]), 2)
        self.assertTrue(payload["answer"].startswith("Dạ, FPT Shop xin chào"))
        for item in payload["recommendations"]:
            self.assertIn("gpu_model", item)
            self.assertIn("reason", item)
            self.assertIn("final_score", item["scores"])

    def test_health_uses_complete_feature_dataset(self):
        response = self.client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["rows"], 417)

    def test_stream_returns_metadata_and_text_events(self):
        response = self.client.post(
            "/chat/stream",
            json={"text": "Laptop văn phòng dưới 20 triệu"},
        )

        self.assertEqual(response.status_code, 200)
        events = [line for line in response.text.splitlines() if line.strip()]
        self.assertGreaterEqual(len(events), 2)
        parsed_events = [json.loads(event) for event in events]
        self.assertEqual(parsed_events[0]["type"], "metadata")
        self.assertTrue(any(event["type"] == "text" for event in parsed_events[1:]))
        self.assertNotIn("NaN", events[0])


if __name__ == "__main__":
    unittest.main()
