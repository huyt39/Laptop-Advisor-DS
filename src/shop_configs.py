from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "shops"

DEFAULT_PRODUCT_LINK_SELECTOR = ".product-item a, .product-info a, .product-name a, a[href*='.html']"
DEFAULT_LOAD_MORE_SELECTORS = [
    ".btn-show-more",
    ".show-more",
    ".view-more",
    ".btn-loadmore",
    "button[aria-label*='xem thêm' i]",
    "button[title*='xem thêm' i]",
]


def _load_shop_file(shop: str) -> Dict[str, Any]:
    path = CONFIG_DIR / f"{shop.lower()}.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def get_brand_url(brand: str = "all") -> Optional[str]:
    cfg = _load_shop_file("fpt")
    brands = cfg.get("brands", {})
    if not isinstance(brands, dict):
        return None
    return brands.get(brand.lower())


def get_product_link_selector() -> str:
    cfg = _load_shop_file("fpt")
    selector = cfg.get("product_link_selector")
    if isinstance(selector, str) and selector.strip():
        return selector
    return DEFAULT_PRODUCT_LINK_SELECTOR


def get_load_more_selectors() -> List[str]:
    cfg = _load_shop_file("fpt")
    selectors = cfg.get("load_more_selectors")
    if isinstance(selectors, list) and selectors:
        return [x for x in selectors if isinstance(x, str) and x.strip()]
    return DEFAULT_LOAD_MORE_SELECTORS
