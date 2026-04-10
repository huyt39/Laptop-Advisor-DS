from __future__ import annotations

import argparse
import json
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from shop_configs import (
    DEFAULT_LOAD_MORE_SELECTORS,
    DEFAULT_PRODUCT_LINK_SELECTOR,
    get_load_more_selectors,
    get_product_link_selector,
    get_brand_url,
    list_shops,
)
from az_no_db import extract_features


def create_driver(headless: bool = True) -> webdriver.Chrome:
    options = Options()
    if headless:
        # Headless mode for automation environments.
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def _normalize_url(href: str, base_url: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("#") or href.lower().startswith("javascript:"):
        return None
    full = urljoin(base_url, href)
    p = urlparse(full)
    if not p.scheme.startswith("http"):
        return None
    return full


def _is_likely_product_url(url: str, shop: Optional[str]) -> bool:
    p = urlparse(url)
    path = p.path.lower()
    if shop == "anphat":
        return path.endswith(".html") and "_id" not in path and "/tim" not in path and "/collection/" not in path
    if shop == "fpt":
        if not path.startswith("/may-tinh-xach-tay/"):
            return False
        slug = path.rsplit("/", 1)[-1]
        # Product slug on FPT usually contains model digits.
        if not any(ch.isdigit() for ch in slug):
            return False
        return True
    return True


def _click_first_available_load_more(
    driver: webdriver.Chrome,
    selectors: List[str],
    timeout_sec: int,
) -> bool:
    for selector in selectors:
        try:
            button = WebDriverWait(driver, timeout_sec).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(0.5)
            button.click()
            return True
        except TimeoutException:
            continue
        except Exception:
            continue
    return False


def crawl_dynamic_links(
    category_url: str,
    product_link_selector: str,
    shop: Optional[str] = None,
    load_more_selectors: Optional[List[str]] = None,
    max_clicks: int = 30,
    wait_after_scroll: float = 1.5,
    wait_after_click: float = 2.0,
    headless: bool = True,
) -> List[str]:
    selectors = load_more_selectors or DEFAULT_LOAD_MORE_SELECTORS
    driver = create_driver(headless=headless)
    try:
        driver.get(category_url)
        time.sleep(2.5)

        for _ in range(max_clicks):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(wait_after_scroll)

            clicked = _click_first_available_load_more(driver, selectors, timeout_sec=4)
            if not clicked:
                break
            time.sleep(wait_after_click)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        links: List[str] = []
        for a in soup.select(product_link_selector):
            href = a.get("href")
            normalized = _normalize_url(href, category_url) if href else None
            if not normalized:
                continue
            if urlparse(normalized).netloc != urlparse(category_url).netloc:
                continue
            if not _is_likely_product_url(normalized, shop):
                continue
            links.append(normalized)
        return list(dict.fromkeys(links))
    finally:
        driver.quit()


def _to_int_price(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    for token in re.finditer(r"\d[\d\.,]{2,}", text):
        raw = token.group(0)
        digits = re.sub(r"[^0-9]", "", raw)
        if not digits:
            continue
        try:
            value = int(digits)
        except ValueError:
            continue
        if 1_000_000 <= value <= 300_000_000:
            return value
    return None


def _extract_price_generic(soup: BeautifulSoup, selectors: List[str]) -> Tuple[Optional[str], Optional[int]]:
    # 0) JSON-LD / embedded structured data (most stable for FPT + An Phat)
    html = str(soup)
    m = re.search(r'"priceCurrency"\s*:\s*"VND"[^{}]{0,200}"price"\s*:\s*"?(\d[\d\.]*)"?', html, re.I)
    if not m:
        m = re.search(r'"price"\s*:\s*"?(\d[\d\.]*)"?', html, re.I)
    if m:
        raw = m.group(1)
        value = _to_int_price(raw)
        if value is not None:
            return raw, value

    meta = soup.select_one("meta[itemprop='price'], meta[property='product:price:amount'], meta[name='price']")
    if meta and meta.get("content"):
        raw = meta["content"].strip()
        value = _to_int_price(raw)
        if value is not None:
            return raw, value

    candidates: List[tuple[str, int]] = []
    for sel in selectors:
        for el in soup.select(sel):
            txt = el.get_text(" ", strip=True)
            if not txt:
                continue
            lowered = txt.lower()
            if any(x in lowered for x in ("trả góp", "voucher", "khuyến mãi", "ưu đãi", "tiết kiệm")):
                continue
            value = _to_int_price(txt)
            if value is not None:
                candidates.append((txt, value))
        if candidates:
            break
    if not candidates:
        return None, None
    # Prefer lower candidate in selector block (often promo/sale price).
    best = min(candidates, key=lambda x: x[1])
    return best[0], best[1]


def _extract_specs_common(soup: BeautifulSoup) -> Dict[str, str]:
    specs: Dict[str, str] = {}
    table = soup.find("table")
    if table:
        for row in table.find_all("tr"):
            cols = row.find_all(["td", "th"])
            if len(cols) >= 2:
                k = cols[0].get_text(" ", strip=True)
                v = cols[1].get_text(" ", strip=True)
                if k:
                    specs[k] = v
    if not specs:
        for dl in soup.find_all("dl"):
            for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
                k = dt.get_text(strip=True)
                v = dd.get_text(strip=True)
                if k:
                    specs[k] = v
    if not specs:
        for li in soup.find_all("li"):
            text = li.get_text(" ", strip=True)
            if ":" in text:
                k, v = [p.strip() for p in text.split(":", 1)]
                if k:
                    specs[k] = v
    return specs


def parse_product_page_anphat(html: str, url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    name = ""
    og = soup.select_one("meta[property='og:title'], meta[name='title']")
    if og and og.get("content"):
        name = og["content"].strip()
    else:
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)
    price_raw, price = _extract_price_generic(
        soup,
        selectors=[
            "[itemprop='price']",
            ".price-main",
            ".product-price",
            ".p-price",
            ".price",
            "[class*='price']",
            "[class*='gia']",
        ],
    )
    image = None
    ogi = soup.select_one("meta[property='og:image']")
    if ogi and ogi.get("content"):
        image = ogi["content"]
    specs = _extract_specs_common(soup)
    features = extract_features(name, specs, price)
    return {
        "url": url,
        "name": name,
        "price": price,
        "price_raw": price_raw,
        "image": image,
        "specs": specs,
        "features": features,
    }


def parse_product_page_fpt(html: str, url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    name = ""
    og = soup.select_one("meta[property='og:title'], meta[name='title']")
    if og and og.get("content"):
        name = og["content"].strip()
    else:
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

    price_raw, price = _extract_price_generic(
        soup,
        selectors=[
            "[itemprop='price']",
            ".st-price-main",
            ".price",
            ".price-current",
            ".product__price",
            "[class*='price']",
            "[class*='gia']",
        ],
    )

    image = None
    ogi = soup.select_one("meta[property='og:image']")
    if ogi and ogi.get("content"):
        image = ogi["content"]

    specs = _extract_specs_common(soup)

    features = extract_features(name, specs, price)
    return {
        "url": url,
        "name": name,
        "price": price,
        "price_raw": price_raw,
        "image": image,
        "specs": specs,
        "features": features,
    }


def parse_product_page(html: str, url: str, shop: Optional[str]) -> Dict:
    if shop == "anphat":
        return parse_product_page_anphat(html, url)
    if shop == "fpt":
        return parse_product_page_fpt(html, url)
    return parse_product_page_fpt(html, url)


def crawl_and_parse_products(urls: List[str], shop: Optional[str], request_delay: float = 0.1) -> List[Dict]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        }
    )
    items: List[Dict] = []
    for u in urls:
        try:
            r = session.get(u, timeout=20)
            if r.status_code != 200:
                continue
            item = parse_product_page(r.text, u, shop=shop)
            if not item.get("name"):
                continue
            items.append(item)
            time.sleep(request_delay)
        except Exception:
            continue
    return items


def main() -> None:
    shop_choices = list_shops()
    parser = argparse.ArgumentParser(description="Crawl dynamic product links via Selenium.")
    parser.add_argument("--url", help="Category/listing URL")
    parser.add_argument("--shop", choices=shop_choices, help="Shop preset name")
    parser.add_argument("--brand", help="Brand name in shop preset (asus/hp/...)")
    parser.add_argument(
        "--product-selector",
        help="CSS selector for product links",
    )
    parser.add_argument(
        "--fallback-product-selector",
        default=DEFAULT_PRODUCT_LINK_SELECTOR,
        help="CSS selector for product links",
    )
    parser.add_argument(
        "--load-more-selector",
        action="append",
        dest="load_more_selectors",
        help="CSS selector for load-more button (can repeat multiple times)",
    )
    parser.add_argument("--max-clicks", type=int, default=30, help="Max load-more clicks")
    parser.add_argument("--show-browser", action="store_true", help="Show browser window")
    parser.add_argument("--full-parse", action="store_true", help="Fetch product pages and parse details")
    parser.add_argument("--out", help="Output JSON path")
    args = parser.parse_args()

    category_url = args.url
    if not category_url and args.shop and args.brand:
        category_url = get_brand_url(args.shop, args.brand)
    if not category_url:
        raise SystemExit("Please provide --url or use --shop with --brand.")

    product_selector = args.product_selector
    if not product_selector and args.shop:
        product_selector = get_product_link_selector(args.shop)
    if not product_selector:
        product_selector = args.fallback_product_selector

    load_more_selectors = args.load_more_selectors
    if not load_more_selectors and args.shop:
        load_more_selectors = get_load_more_selectors(args.shop)

    urls = crawl_dynamic_links(
        category_url=category_url,
        product_link_selector=product_selector,
        shop=args.shop,
        load_more_selectors=load_more_selectors,
        max_clicks=args.max_clicks,
        headless=not args.show_browser,
    )

    if args.full_parse:
        items = crawl_and_parse_products(urls, shop=args.shop)
        out = args.out or "data/dynamic_products.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        print(f"Parsed {len(items)} products and saved to {out}")
        return

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(urls, f, ensure_ascii=False, indent=2)
        print(f"Collected {len(urls)} product URLs and saved to {args.out}")
        return

    print(f"Collected {len(urls)} product URLs")
    for u in urls[:10]:
        print(u)


if __name__ == "__main__":
    main()
