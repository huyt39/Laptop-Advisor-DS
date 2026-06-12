import unittest

import pandas as pd

from src.advisor.features import _cpu_score, _gpu_score, prepare_laptop_dataframe


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


if __name__ == "__main__":
    unittest.main()
