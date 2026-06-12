import unittest

import pandas as pd

from src.advisor.features import _cpu_score, _gpu_score, prepare_laptop_dataframe
from src.feature_extractor import extract_features


class FeatureEngineeringTests(unittest.TestCase):
    def test_gpu_model_hierarchy_is_not_overwritten_by_generic_rules(self):
        frame = pd.DataFrame(
            [
                {
                    "Product Name": "Laptop gaming NVIDIA",
                    "GPU manufacturer": "NVIDIA",
                    "GPU model": "GeForce RTX 5060",
                    "GPU type": "Dedicated",
                },
                {
                    "Product Name": "Laptop gaming NVIDIA",
                    "GPU manufacturer": "NVIDIA",
                    "GPU model": "GeForce RTX 3060",
                    "GPU type": "Dedicated",
                },
                {
                    "Product Name": "Laptop van phong",
                    "GPU manufacturer": "Intel",
                    "GPU model": "Intel UHD",
                    "GPU type": "Integrated",
                },
            ]
        )

        scores = _gpu_score(frame)

        self.assertEqual(scores.iloc[0], 1.0)
        self.assertGreater(scores.iloc[0], scores.iloc[1])
        self.assertGreater(scores.iloc[1], scores.iloc[2])

    def test_apple_m5_is_recognized_as_apple_silicon(self):
        frame = pd.DataFrame(
            {
                "CPU brand modifier": ["Apple M5", ""],
                "CPU generation": [None, None],
                "CPU Speed (GHz)": [None, None],
            }
        )

        scores = _cpu_score(frame)

        self.assertGreater(scores.iloc[0], scores.iloc[1])

    def test_ultrabook_requires_observed_weight_and_battery(self):
        frame = pd.DataFrame(
            [
                {
                    "Product Name": "Thin laptop without measured portability",
                    "Price (VND)": 20_000_000,
                    "RAM (GB)": 16,
                    "Storage (GB)": 512,
                    "Screen Size (inch)": 14,
                }
            ]
        )

        prepared = prepare_laptop_dataframe(frame, fpt_only=False)

        self.assertFalse(bool(prepared.loc[0, "is_ultrabook"]))
        self.assertFalse(bool(prepared.loc[0, "is_light"]))

    def test_gpu_model_prefers_sku_and_highlight_over_conflicting_card_field(self):
        title = "Laptop Gaming 16GB/512GB/NVIDIA GeForce RTX5050 8GB"
        specs = {
            "Thông số nổi bật 1": "NVIDIA GeForce RTX 5050",
            "Card đồ hoạ": "NVIDIA GeForce RTX 5070 8GB GDDR7",
        }

        features = extract_features(title, specs, 30_000_000)

        self.assertEqual(features["GPU model"], "RTX 5050")


if __name__ == "__main__":
    unittest.main()
