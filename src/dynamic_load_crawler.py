from __future__ import annotations

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from feature_extractor import extract_features
from shop_configs import get_load_more_selectors, get_product_link_selector


FPT_CATEGORY_URL = "https://fptshop.com.vn/may-tinh-xach-tay"
FPT_OUTPUT_JSON = Path("data/fpt_laptops.json")
FPT_CATEGORY_API = "https://papi.fptshop.com.vn/gw/v1/public/fulltext-search-service/category"
FPT_CATEGORY_SLUG = "may-tinh-xach-tay"
FPT_API_BATCH_SIZE = 24
FPT_API_PRODUCT_METADATA: Dict[str, Dict] = {}


def create_driver(headless: bool = True) -> webdriver.Chrome:
    options = Options()
    if headless:
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
    parsed = urlparse(full)
    if not parsed.scheme.startswith("http"):
        return None
    return full


def _is_fpt_product_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    if not path.startswith("/may-tinh-xach-tay/"):
        return False
    slug = path.rsplit("/", 1)[-1]
    category_like = {
        "gaming-do-hoa",
        "asus",
        "lenovo",
        "hp",
        "acer",
        "msi",
        "gigabyte",
        "apple-macbook",
        "lg",
        "dell",
        "samsung",
        "colorful",
        "masstel",
        "sinh-vien-van-phong",
        "mong-nhe",
        "doanh-nhan",
        "ai",
    }
    return slug not in category_like


def _collect_fpt_links_from_html(html: str, category_url: str, product_link_selector: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    out: Dict[str, bool] = {}
    for a in soup.select(product_link_selector):
        href = a.get("href")
        normalized = _normalize_url(href, category_url) if href else None
        if not normalized:
            continue
        if urlparse(normalized).netloc != urlparse(category_url).netloc:
            continue
        if not _is_fpt_product_url(normalized):
            continue
        out[normalized] = True

    # FPT Shop is a Next.js app. Some product links are present only inside
    # streamed script payloads, so a light regex pass recovers links that are
    # not materialized as anchors in the current DOM snapshot.
    for match in re.finditer(r'"slug"\s*:\s*"([^"]*may-tinh-xach-tay/[^"]+)"', html):
        normalized = _normalize_url(match.group(1), category_url)
        if normalized and _is_fpt_product_url(normalized):
            out[normalized] = True
    for match in re.finditer(r'(?<![\w/-])may-tinh-xach-tay/[a-zA-Z0-9?=&._%-]+', html):
        normalized = _normalize_url(match.group(0), category_url)
        if normalized and _is_fpt_product_url(normalized):
            out[normalized] = True
    return list(out.keys())


def _api_product_to_urls(product: Dict) -> List[str]:
    urls: List[str] = []
    skus = product.get("skus")
    if isinstance(skus, list):
        for sku in skus:
            if isinstance(sku, dict) and sku.get("slug"):
                url = _normalize_url(str(sku["slug"]), FPT_CATEGORY_URL)
                if url:
                    urls.append(url)
    if urls:
        return list(dict.fromkeys(urls))

    slug = product.get("slug")
    if isinstance(slug, str) and slug:
        url = _normalize_url(slug, FPT_CATEGORY_URL)
        if url:
            urls.append(url)
    return list(dict.fromkeys(urls))


def _extract_api_specs(product: Dict, sku: Optional[Dict] = None) -> Dict[str, str]:
    specs: Dict[str, str] = {}
    sku_name = ""
    if isinstance(sku, dict):
        sku_name = str(sku.get("displayName") or sku.get("name") or sku.get("shortDisplayName") or "").strip()
    product_name = str(product.get("displayName") or product.get("name") or "").strip()
    if sku_name:
        specs["Tên SKU"] = sku_name
    elif product_name:
        specs["Tên sản phẩm API"] = product_name

    for idx, point in enumerate(product.get("keySellingPoints") or [], start=1):
        if not isinstance(point, dict):
            continue
        title = str(point.get("title") or "").strip()
        description = str(point.get("description") or "").strip()
        if title or description:
            specs[f"Thông số nổi bật {idx}"] = " ".join(x for x in (title, description) if x)
    return specs


def _cache_api_metadata(product: Dict, url: str, sku: Optional[Dict] = None) -> None:
    metadata = {
        "name": (sku or {}).get("displayName") or (sku or {}).get("name") or product.get("displayName") or product.get("name"),
        "price": (sku or {}).get("currentPrice") or product.get("currentPrice") or product.get("price"),
        "original_price": (sku or {}).get("originalPrice") or product.get("originalPrice"),
        "image": ((product.get("image") or {}).get("src") if isinstance(product.get("image"), dict) else product.get("image"))
        or (sku or {}).get("image"),
        "brand": ((product.get("brand") or {}).get("name") if isinstance(product.get("brand"), dict) else None),
        "specs": _extract_api_specs(product, sku),
    }
    FPT_API_PRODUCT_METADATA[url] = metadata


def crawl_fpt_links_via_api(max_batches: int = 50, batch_size: int = FPT_API_BATCH_SIZE) -> List[str]:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://fptshop.com.vn",
            "Referer": FPT_CATEGORY_URL,
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        }
    )

    collected: Dict[str, bool] = {}
    total_count: Optional[int] = None
    for batch_idx in range(max_batches):
        skip_count = batch_idx * batch_size
        payload = {
            "slug": FPT_CATEGORY_SLUG,
            "skipCount": skip_count,
            "maxResultCount": batch_size,
            "categoryType": "category",
        }
        try:
            response = session.post(FPT_CATEGORY_API, json=payload, timeout=20)
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            print(f"  FPT category API failed at skip={skip_count}: {exc}")
            break

        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list) or not items:
            break
        if isinstance(data.get("totalCount"), int):
            total_count = data["totalCount"]

        before_count = len(collected)
        for product in items:
            if not isinstance(product, dict):
                continue
            skus = product.get("skus")
            sku_by_url: Dict[str, Optional[Dict]] = {}
            if isinstance(skus, list):
                for sku in skus:
                    if isinstance(sku, dict) and sku.get("slug"):
                        sku_url = _normalize_url(str(sku["slug"]), FPT_CATEGORY_URL)
                        if sku_url:
                            sku_by_url[sku_url] = sku
            for url in _api_product_to_urls(product):
                if url and _is_fpt_product_url(url):
                    collected[url] = True
                    _cache_api_metadata(product, url, sku_by_url.get(url))

        print(f"  API batch {batch_idx + 1}: collected {len(collected)} links")
        if len(collected) == before_count:
            break
        if total_count is not None and len(collected) >= total_count:
            break

    return list(collected.keys())


def _click_first_available_load_more(driver: webdriver.Chrome, selectors: List[str], timeout_sec: int) -> bool:
    # Prefer the real category load-more button. Other "xem thêm" controls may
    # appear in banners/articles and do not load product cards.
    text_xpath = (
        "//*[self::button or self::a or self::span or self::div]"
        "[contains(translate(normalize-space(.), "
        "'ABCDEFGHIJKLMNOPQRSTUVWXYZÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬĐÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴ', "
        "'abcdefghijklmnopqrstuvwxyzàáảãạăằắẳẵặâầấẩẫậđèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵ'), "
        "'xem thêm') "
        "and (contains(translate(normalize-space(.), "
        "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'sản phẩm') "
        "or contains(translate(normalize-space(.), "
        "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'san pham'))]"
    )
    try:
        button = WebDriverWait(driver, timeout_sec).until(EC.element_to_be_clickable((By.XPATH, text_xpath)))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", button)
        return True
    except Exception:
        pass

    for selector in selectors:
        try:
            button = WebDriverWait(driver, timeout_sec).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", button)
            return True
        except TimeoutException:
            continue
        except Exception:
            continue

    xpath_candidates = [
        "//button[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'xem thêm')]",
        "//a[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'xem thêm')]",
        "//span[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'xem thêm')]",
        "//button[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'xem them')]",
        "//a[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'xem them')]",
        "//span[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'xem them')]",
    ]
    for xpath in xpath_candidates:
        try:
            button = WebDriverWait(driver, timeout_sec).until(EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", button)
            return True
        except Exception:
            continue
    return False


def crawl_fpt_links(
    category_url: str = FPT_CATEGORY_URL,
    max_clicks: int = 30,
    max_idle_rounds: int = 4,
    wait_after_scroll: float = 1.5,
    wait_after_click: float = 2.0,
    headless: bool = True,
) -> List[str]:
    api_links = crawl_fpt_links_via_api(max_batches=max_clicks)
    if api_links:
        print(f"  Collected {len(api_links)} FPT links via category API.")
        return api_links

    print("  Falling back to Selenium category crawl.")
    product_link_selector = get_product_link_selector()
    load_more_selectors = get_load_more_selectors()

    driver = create_driver(headless=headless)
    try:
        driver.get(category_url)
        time.sleep(2.5)
        collected: Dict[str, bool] = {}
        idle_rounds = 0

        for click_idx in range(max_clicks):
            # Scroll in several steps so lazy product cards have time to render
            # before we inspect the page or click the load-more button.
            for ratio in (0.35, 0.7, 1.0):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight * arguments[0]);", ratio)
                time.sleep(wait_after_scroll / 3)

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(wait_after_scroll)

            before_count = len(collected)
            for link in _collect_fpt_links_from_html(driver.page_source, category_url, product_link_selector):
                collected[link] = True

            clicked = _click_first_available_load_more(driver, load_more_selectors, timeout_sec=4)
            if clicked:
                time.sleep(wait_after_click)
                for link in _collect_fpt_links_from_html(driver.page_source, category_url, product_link_selector):
                    collected[link] = True
                print(f"  Load-more {click_idx + 1}: collected {len(collected)} links")

            if len(collected) == before_count and not clicked:
                idle_rounds += 1
            else:
                idle_rounds = 0
            if idle_rounds >= max_idle_rounds:
                break

        return list(collected.keys())
    finally:
        driver.quit()


def _to_int_price(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    for token in re.finditer(r"\d[\d\.,]{2,}", text):
        digits = re.sub(r"[^0-9]", "", token.group(0))
        if not digits:
            continue
        try:
            value = int(digits)
        except ValueError:
            continue
        if 1_000_000 <= value <= 300_000_000:
            return value
    return None


def _extract_price(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[int]]:
    html = str(soup)
    match = re.search(r'"priceCurrency"\s*:\s*"VND"[^{}]{0,200}"price"\s*:\s*"?(\d[\d\.]*)"?', html, re.I)
    if not match:
        match = re.search(r'"price"\s*:\s*"?(\d[\d\.]*)"?', html, re.I)
    if match:
        raw = match.group(1)
        value = _to_int_price(raw)
        if value is not None:
            return raw, value

    meta = soup.select_one("meta[itemprop='price'], meta[property='product:price:amount'], meta[name='price']")
    if meta and meta.get("content"):
        raw = meta["content"].strip()
        value = _to_int_price(raw)
        if value is not None:
            return raw, value

    selectors = [
        "[itemprop='price']",
        ".st-price-main",
        ".price-current",
        ".product__price",
        ".price",
        "[class*='price']",
        "[class*='gia']",
    ]
    candidates: List[tuple[str, int]] = []
    for selector in selectors:
        for el in soup.select(selector):
            text = el.get_text(" ", strip=True)
            if not text:
                continue
            lowered = text.lower()
            if any(x in lowered for x in ("trả góp", "voucher", "khuyến mãi", "ưu đãi", "tiết kiệm")):
                continue
            value = _to_int_price(text)
            if value is not None:
                candidates.append((text, value))
        if candidates:
            break

    if not candidates:
        return None, None
    best = min(candidates, key=lambda x: x[1])
    return best[0], best[1]


_CONTACT_KEYWORDS = frozenset({
    "email",
    "điện thoại",
    "phone",
    "fax",
    "cửa hàng",
    "trung tâm bảo hành",
    "liên hệ",
    "chịu trách nhiệm",
    "hotline",
})


def _table_to_dict(table) -> Dict[str, str]:
    data: Dict[str, str] = {}
    for row in table.find_all("tr"):
        cols = row.find_all(["td", "th"])
        if len(cols) >= 2:
            key = cols[0].get_text(" ", strip=True)
            value = cols[1].get_text(" ", strip=True)
            if key:
                data[key] = value
    return data


def _is_contact_table(kv: Dict[str, str]) -> bool:
    if not kv:
        return True
    hits = sum(1 for key in kv if any(c in key.lower() for c in _CONTACT_KEYWORDS))
    return hits > len(kv) * 0.3


def _is_specs_like(kv: Dict[str, str]) -> bool:
    if not kv:
        return False
    joined = " ".join(kv.keys()).lower()
    spec_keywords = (
        "cpu",
        "chip",
        "ram",
        "ssd",
        "ổ cứng",
        "màn hình",
        "độ phân giải",
        "gpu",
        "card",
        "pin",
        "trọng lượng",
        "kích thước",
        "hệ điều hành",
    )
    return any(keyword in joined for keyword in spec_keywords)


def _extract_json_ld_specs(soup: BeautifulSoup) -> Dict[str, str]:
    specs: Dict[str, str] = {}
    for script in soup.select("script[type='application/ld+json']"):
        raw = script.string or script.get_text("", strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        nodes = payload if isinstance(payload, list) else [payload]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            props = node.get("additionalProperty")
            if not isinstance(props, list):
                continue
            for prop in props:
                if not isinstance(prop, dict):
                    continue
                key = str(prop.get("name") or "").strip()
                value = str(prop.get("value") or "").strip()
                if key and value:
                    specs[key] = value
    return specs


def _extract_specs(soup: BeautifulSoup) -> Dict[str, str]:
    json_ld_specs = _extract_json_ld_specs(soup)
    if _is_specs_like(json_ld_specs):
        return json_ld_specs

    specs: Dict[str, str] = {}
    for container in soup.select(
        "[class*='spec'], [class*='thong-so'], [class*='parameter'], "
        "[class*='config'], [id*='spec'], [id*='thong-so'], "
        ".product-info-table, .product-specs, .box-specifi"
    ):
        for table in container.find_all("table"):
            parsed = _table_to_dict(table)
            if parsed and _is_specs_like(parsed) and not _is_contact_table(parsed):
                specs.update(parsed)
        if specs:
            return specs

    best: Dict[str, str] = {}
    for table in soup.find_all("table"):
        parsed = _table_to_dict(table)
        if parsed and _is_specs_like(parsed) and not _is_contact_table(parsed) and len(parsed) > len(best):
            best = parsed
    if best:
        return best

    for li in soup.find_all("li"):
        text = li.get_text(" ", strip=True)
        if ":" in text and len(text) < 300:
            key, value = [p.strip() for p in text.split(":", 1)]
            if key and len(key) < 80 and not any(c in key.lower() for c in _CONTACT_KEYWORDS):
                specs[key] = value
    return specs if _is_specs_like(specs) else json_ld_specs


def parse_fpt_product_page(html: str, url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    api_metadata = FPT_API_PRODUCT_METADATA.get(url, {})

    name = ""
    og = soup.select_one("meta[property='og:title'], meta[name='title']")
    if og and og.get("content"):
        name = og["content"].strip()
    else:
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

    price_raw, price = _extract_price(soup)

    image = None
    og_image = soup.select_one("meta[property='og:image']")
    if og_image and og_image.get("content"):
        image = og_image["content"]

    specs = _extract_specs(soup)
    api_specs = api_metadata.get("specs")
    if isinstance(api_specs, dict):
        specs = {**api_specs, **specs}

    final_name = str(api_metadata.get("name") or name or "").strip()
    final_price = api_metadata.get("price") or price
    final_image = api_metadata.get("image") or image
    feature_context = f"{final_name} {name} {url} {' '.join(str(v) for v in specs.values())}"
    features = extract_features(feature_context, specs, final_price)
    if api_metadata.get("brand") and not features.get("Manufacturer"):
        features["Manufacturer"] = str(api_metadata["brand"]).lower()

    return {
        "url": url,
        "name": final_name,
        "price": final_price,
        "price_raw": str(final_price) if final_price is not None else price_raw,
        "image": final_image,
        "specs": specs,
        "features": features,
    }


def _is_valid_product(item: Dict) -> bool:
    if not item.get("name"):
        return False
    features = item.get("features") or {}
    filled = sum(1 for v in features.values() if v)
    return item.get("price") is not None and filled >= 3


def _fetch_url(url: str) -> Tuple[str, Optional[str]]:
    try:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
            }
        )
        response = session.get(url, timeout=20)
        if response.status_code == 200:
            return url, response.text
    except Exception:
        pass
    return url, None


def crawl_and_parse_fpt_products(urls: List[str], max_workers: int = 5, save_html: bool = False) -> List[Dict]:
    html_dir: Optional[Path] = None
    if save_html:
        html_dir = Path("data/fpt/raw_htmls")
        html_dir.mkdir(parents=True, exist_ok=True)

    html_map: Dict[str, str] = {}
    print(f"  Fetching {len(urls)} FPT product pages ({max_workers} workers)...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_url, url): url for url in urls}
        for future in as_completed(futures):
            url, html = future.result()
            if html:
                html_map[url] = html

    items: List[Dict] = []
    for idx, (url, html) in enumerate(html_map.items()):
        if save_html and html_dir:
            html_path = html_dir / f"{idx:04d}.html"
            html_path.write_text(html, encoding="utf-8")

        item = parse_fpt_product_page(html, url)
        if not _is_valid_product(item):
            continue
        if save_html and html_dir:
            item["saved_path"] = str(html_dir / f"{idx:04d}.html")
        items.append(item)

    print(f"  Validated {len(items)} FPT products.")
    return items


def save_fpt_products(items: List[Dict], output_path: Path = FPT_OUTPUT_JSON) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
