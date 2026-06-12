from __future__ import annotations

import argparse
import json
from pathlib import Path

from dynamic_load_crawler import FPT_CATEGORY_URL, crawl_and_parse_fpt_products, crawl_fpt_links, save_fpt_products


def main() -> None:
    parser = argparse.ArgumentParser(description="Crawl FPT Shop laptop inventory.")
    parser.add_argument("--max-clicks", type=int, default=30, help="Max load-more clicks.")
    parser.add_argument("--links-only", action="store_true", help="Only collect FPT product links.")
    parser.add_argument("--show-browser", action="store_true", help="Show Chrome window.")
    parser.add_argument("--save-html", action="store_true", help="Save raw FPT product HTML for debugging.")
    parser.add_argument("--out", default="data/fpt_laptops.json", help="Output JSON path.")
    args = parser.parse_args()

    links = crawl_fpt_links(
        category_url=FPT_CATEGORY_URL,
        max_clicks=args.max_clicks,
        headless=not args.show_browser,
    )

    output = Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    if args.links_only:
        output.write_text(json.dumps(links, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Saved {len(links)} FPT links to {output}")
        return

    items = crawl_and_parse_fpt_products(links, save_html=args.save_html)
    save_fpt_products(items, output)
    print(f"Saved {len(items)} FPT products to {output}")


if __name__ == "__main__":
    main()
