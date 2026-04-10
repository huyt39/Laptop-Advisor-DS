from __future__ import annotations

import argparse
import json
import os

from dynamic_load_crawler import crawl_and_parse_products, crawl_dynamic_links
from shop_configs import get_brand_url, get_load_more_selectors, get_product_link_selector, list_shops


def main() -> None:
    shop_choices = list_shops()
    parser = argparse.ArgumentParser(description="Simple runner for Anphat/FPT laptop crawling.")
    parser.add_argument("--shop", choices=shop_choices, required=True, help="Shop name")
    parser.add_argument("--brand", default="all", help="Brand key in shop config (default: all)")
    parser.add_argument("--max-clicks", type=int, default=20, help="Max load-more clicks")
    parser.add_argument("--links-only", action="store_true", help="Only collect product links")
    parser.add_argument("--show-browser", action="store_true", help="Show Chrome window")
    args = parser.parse_args()

    category_url = get_brand_url(args.shop, args.brand)
    if not category_url:
        raise SystemExit(f"Brand '{args.brand}' not found for shop '{args.shop}'.")

    links = crawl_dynamic_links(
        category_url=category_url,
        product_link_selector=get_product_link_selector(args.shop),
        shop=args.shop,
        load_more_selectors=get_load_more_selectors(args.shop),
        max_clicks=args.max_clicks,
        headless=not args.show_browser,
    )

    os.makedirs("data", exist_ok=True)
    if args.links_only:
        out = f"data/{args.shop}_links.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(links, f, ensure_ascii=False, indent=2)
        print(f"Saved {len(links)} links to {out}")
        return

    items = crawl_and_parse_products(links, shop=args.shop)
    out = f"data/{args.shop}_laptops.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(items)} items to {out}")


if __name__ == "__main__":
    main()
