#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests
from dotenv import load_dotenv

from src.advisor.advisor import recommend_laptops
from src.advisor.features import prepare_laptop_dataframe
from src.advisor.recommend_service import recommendations_to_json
from src.api.main import patch_intent_from_text
from src.llm.schemas_v2 import IntentV2
from src.advisor.recommend_service import build_query_from_intent
from src.evaluation.evaluator import RecommendationEvaluator


load_dotenv()


def load_test_queries(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Test query file must contain a JSON list.")
    return data


def call_chat_api(query: str, api_url: str) -> Dict[str, Any]:
    resp = requests.post(api_url, json={"text": query}, headers={"Content-Type": "application/json"}, timeout=60)
    resp.raise_for_status()
    return resp.json()


def recommend_direct(query_text: str, df: pd.DataFrame, top_k: int) -> Dict[str, Any]:
    intent = patch_intent_from_text(query_text, IntentV2(user_type="general", top_n=top_k))
    if not getattr(intent, "top_n", None):
        intent.top_n = top_k
    query = build_query_from_intent(intent)
    top_df = recommend_laptops(df, query, top_n=top_k)
    return {
        "intent": intent.model_dump(exclude_none=True),
        "query": query,
        "recommendations": recommendations_to_json(top_df, query),
    }


def build_test_cases(
    queries: List[Dict[str, Any]],
    *,
    mode: str,
    top_k: int,
    data_path: str,
    api_url: str,
) -> List[Dict[str, Any]]:
    df = None
    if mode == "direct":
        raw = pd.read_csv(data_path)
        df = prepare_laptop_dataframe(raw, fpt_only=True)

    cases: List[Dict[str, Any]] = []
    for item in queries:
        text = item["query"]
        if mode == "api":
            response = call_chat_api(text, api_url)
        else:
            response = recommend_direct(text, df, top_k)
        cases.append(
            {
                "id": item.get("id"),
                "query": text,
                "recommendations": response.get("recommendations", []),
                "expected_constraints": item.get("expected_constraints", {}),
                "api_intent": response.get("intent"),
                "query_obj": response.get("query"),
                "complexity": item.get("complexity"),
                "note": item.get("note"),
            }
        )
    return cases


def save_metrics_summary(result: Dict[str, Any], output_path: str) -> Dict[str, Any]:
    if "aggregate" in result:
        agg = result["aggregate"]
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "num_queries": result["num_queries"],
            "top_k": result["top_k"],
            "metrics": {
                "precision_at_k": agg["avg_precision_at_k"],
                "strict_precision_at_k": agg["avg_strict_precision_at_k"],
                "normalized_relevance_at_k": agg["avg_normalized_relevance_at_k"],
                "full_match_query_rate": agg["full_match_query_rate"],
                "ndcg_at_k": agg["avg_ndcg_at_k"],
                "mrr": agg["mrr"],
                "csr": agg["avg_csr"],
                "unique_name_rate": agg["avg_unique_name_rate"],
            },
        }
    else:
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "query": result["query"],
            "top_k": result["top_k"],
            "metrics": {
                "precision_at_k": result["precision_at_k"],
                "ndcg_at_k": result["ndcg_at_k"],
                "csr": result.get("csr"),
            },
            "relevances": result["relevances"],
        }
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    return metrics


def print_batch_result(result: Dict[str, Any]) -> None:
    agg = result["aggregate"]
    print(f"Evaluated {result['num_queries']} queries, Top-{result['top_k']}")
    print(f"Precision@K: {agg['avg_precision_at_k']:.2%}")
    print(f"Strict Precision@K: {agg['avg_strict_precision_at_k']:.2%}")
    print(f"Normalized Relevance@K: {agg['avg_normalized_relevance_at_k']:.2%}")
    print(f"Full-match Query Rate: {agg['full_match_query_rate']:.2%}")
    print(f"NDCG@K: {agg['avg_ndcg_at_k']:.4f}")
    print(f"MRR: {agg['mrr']:.4f}")
    if agg["avg_csr"] is not None:
        print(f"CSR: {agg['avg_csr']:.2%}")
    print(f"Unique Name Rate: {agg['avg_unique_name_rate']:.2%}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark the FPT Shop Laptop Advisor.")
    parser.add_argument("--file", "-f", default="data/fpt_test_queries.json", help="JSON test query file.")
    parser.add_argument("--query", "-q", help="Evaluate one query instead of a file.")
    parser.add_argument("--mode", choices=["direct", "api"], default="direct", help="Use internal recommender or HTTP API.")
    parser.add_argument("--data-path", default=os.getenv("LAPTOPS_CSV", "data/fpt_laptops_features.csv"))
    parser.add_argument("--api-url", default="http://localhost:8000/chat")
    parser.add_argument("--top-k", type=int, default=3)
    parser.add_argument("--judge", choices=["rule", "gemini"], default="rule")
    parser.add_argument("--output", "-o", default="data/fpt_evaluation_results.json")
    parser.add_argument("--metrics-output", "-m", default="data/fpt_metrics_summary.json")
    parser.add_argument("--sleep", type=float, default=1.5)
    args = parser.parse_args()

    if args.query:
        queries = [{"id": 1, "query": args.query, "expected_constraints": {}}]
    else:
        queries = load_test_queries(args.file)

    cases = build_test_cases(
        queries,
        mode=args.mode,
        top_k=args.top_k,
        data_path=args.data_path,
        api_url=args.api_url,
    )
    evaluator = RecommendationEvaluator(top_k=args.top_k, judge_mode=args.judge, sleep_between_calls=args.sleep)
    result = evaluator.evaluate_batch(cases)

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    save_metrics_summary(result, args.metrics_output)
    print_batch_result(result)
    print(f"Full results: {args.output}")
    print(f"Metrics: {args.metrics_output}")


if __name__ == "__main__":
    main()
