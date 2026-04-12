from __future__ import annotations

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


CATEGORY_URL = 'https://laptopaz.vn/laptop-moi.html'
MAX_PAGES = 20
OUTPUT_PATH = 'data/az_laptops.json'


def create_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    return s


def parse_category_page(html: str, base: str) -> List[str]:
    soup = BeautifulSoup(html, 'html.parser')
    urls: List[str] = []
    # Additional common containers used by ecommerce themes
    alt_containers = [
        ("div", {"class": "product-list"}),
        ("div", {"class": "product-grid"}),
        ("ul", {"class": "products"}),
        ("div", {"class": "product-item"}),
        ("div", {"class": "item"}),

    ]
    for tag, attrs in alt_containers:
        c = soup.find(tag, attrs)
        if c:
            for a in c.select('a'):
                href = a.get('href')
                if not href:
                    continue
                if href.startswith('/'):
                    href = base.rstrip('/') + href
                urls.append(href)
            if urls:
                return list(dict.fromkeys(urls))

    # More robust fallback: scan anchors and use heuristics to pick laptop/product links
    for a in soup.select('a'):
        href = a.get('href')
        if not href:
            continue
        href = href.strip()
        # normalize
        if href.startswith('/'):
            href = base.rstrip('/') + href
        # ignore external
        parsed = requests.utils.urlparse(href)
        if parsed.netloc and parsed.netloc != requests.utils.urlparse(base).netloc:
            continue
        # heuristics: prefer links that contain laptop-related slugs or product id pattern
        if re.search(r"_dm\d+\.html", href) or re.search(r"/(?:may-tinh|may-tinh-xach-tay|laptop|notebook)/", href, re.I) or 'may-tinh' in href.lower() or 'laptop' in href.lower():
            urls.append(href)
            continue
        # also accept anchors inside product-item like elements
        parent = a
        for _ in range(6):
            try:
                parent = getattr(parent, 'parent', None)
                if not parent:
                    break
                cls_attr = parent.get('class') or [] if hasattr(parent, 'get') else []
                # Normalize class string
                cls = ' '.join(cls_attr).lower()
                # Use simple substring checks instead of regex to avoid heavy compilation on large strings
                keywords = ('product', 'p-item', 'product-item', 'product-card', 'p-list-container', 'p-item')
                if any(k in cls for k in keywords):
                    urls.append(href)
                    break
            except Exception:
                # defensive: don't let a single problematic node stop parsing
                break

    # deduplicate while preserving order
    return list(dict.fromkeys(urls))


def parse_product_page(html: str, url: str) -> Dict:
    soup = BeautifulSoup(html, 'html.parser')
    # name
    name = ''
    og = soup.select_one("meta[property='og:title'], meta[name='title']")
    if og and og.get('content'):
        name = og['content'].strip()
    else:
        h1 = soup.find('h1')
        if h1:
            name = h1.get_text(strip=True)

    # price
    price_raw, price = extract_price(soup)

    # image
    image = None
    ogi = soup.select_one("meta[property='og:image']")
    if ogi and ogi.get('content'):
        image = ogi['content']

    # specs table / lists
    specs: Dict[str, str] = {}
    table = soup.find('table')
    if table:
        for row in table.find_all('tr'):
            cols = row.find_all(['td', 'th'])
            if len(cols) >= 2:
                k = cols[0].get_text(' ', strip=True)
                v = cols[1].get_text(' ', strip=True)
                specs[k] = v

    if not specs:
        for dl in soup.find_all('dl'):
            dts = dl.find_all('dt')
            dds = dl.find_all('dd')
            for dt, dd in zip(dts, dds):
                k = dt.get_text(strip=True)
                v = dd.get_text(strip=True)
                specs[k] = v

    if not specs:
        for li in soup.find_all('li'):
            text = li.get_text(' ', strip=True)
            if ':' in text:
                k, v = [p.strip() for p in text.split(':', 1)]
                specs[k] = v

    features = extract_features(name, specs, price)

    return {
        'url': url,
        'name': name,
        'price': price,
        'price_raw': price_raw,
        'image': image,
        'specs': specs,
        'features': features,
    }


def extract_price(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[int]]:
    # 1) structured/meta price if available
    meta_price = soup.select_one("meta[itemprop='price'], meta[property='product:price:amount'], meta[name='price']")
    if meta_price and meta_price.get('content'):
        raw = meta_price['content'].strip()
        n = to_int_price(raw)
        if n is not None:
            return raw, n

    # 2) common product-price containers on Vietnamese ecommerce pages
    # Order matters: try specific selectors first.
    selectors = [
        ".product-price .price",
        ".product-price",
        ".product-detail-price",
        ".price-box .special-price",
        ".special-price .price",
        ".price-current",
        ".price-sale",
        ".gia-ban",
        ".price",
        "[class*='price']",
    ]
    candidates: List[tuple[str, int]] = []
    for sel in selectors:
        for el in soup.select(sel):
            txt = el.get_text(' ', strip=True)
            if not txt:
                continue
            # Reduce false positives like installments/accessories.
            lowered = txt.lower()
            if any(x in lowered for x in ('trả góp', 'tháng', 'phụ kiện', 'voucher')):
                continue
            value = to_int_price(txt)
            # Skip tiny values that are often accessory prices.
            if value is not None and value >= 1_000_000:
                candidates.append((txt, value))
        if candidates:
            break

    # 3) fallback from full page text
    if not candidates:
        text = soup.get_text(' ', strip=True)
        for m in re.finditer(r"(\d[\d\.,]{5,})\s*(đ|vnđ|vnd)", text, re.I):
            raw = m.group(0)
            value = to_int_price(raw)
            if value is not None and value >= 1_000_000:
                candidates.append((raw, value))

    if not candidates:
        return None, None

    # Choose the highest value within the selected block to reduce accessory-price noise.
    best = max(candidates, key=lambda x: x[1])
    return best[0], best[1]


def to_int_price(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    # Extract each monetary token independently (avoid joining multiple prices).
    for token in re.finditer(r"\d[\d\.,]{2,}", text):
        raw = token.group(0)
        digits = re.sub(r"[^0-9]", "", raw)
        if not digits:
            continue
        try:
            value = int(digits)
        except ValueError:
            continue
        # Keep practical laptop range to avoid accessory/noise values.
        if 1_000_000 <= value <= 200_000_000:
            return value
    return None


def extract_features(title: str, specs: Dict[str, str], price_value: Optional[int]) -> Dict[str, Optional[str]]:
    """Extract normalised laptop features from product title + spec dict.

    Uses a *spec-dict-first* strategy: each field is first looked up by matching
    spec keys before falling back to the product title.  This avoids the previous
    bug where a single greedy regex on the concatenated text would mis-assign
    values (e.g. grabbing RAM size as Storage).
    """
    fields = [
        'Manufacturer', 'CPU manufacturer', 'CPU brand modifier', 'CPU generation',
        'CPU Speed (GHz)', 'RAM (GB)', 'RAM Type', 'Bus (MHz)', 'Storage (GB)',
        'Screen Size (inch)', 'Screen Resolution', 'Refresh Rate (Hz)',
        'GPU manufacturer', 'Weight (kg)', 'Battery', 'Price (VND)',
    ]
    out: Dict[str, Optional[str]] = {k: None for k in fields}
    title_l = (title or '').lower()

    # ---- helpers ----
    def _find_spec(*keywords: str) -> Optional[str]:
        for k, v in specs.items():
            kl = k.lower()
            for kw in keywords:
                if kw in kl and v and v.strip():
                    return v.strip()
        return None

    def _spec_or_title(*keywords: str) -> str:
        return _find_spec(*keywords) or title or ''

    # ---- Manufacturer ----
    brand_spec = _find_spec('hãng', 'thương hiệu', 'brand', 'manufacturer')
    if brand_spec:
        out['Manufacturer'] = brand_spec
    else:
        _mfr = [
            ('asus', 'asus'), ('lenovo', 'lenovo'), ('dell', 'dell'),
            ('acer', 'acer'), ('msi ', 'msi'), ('msi-', 'msi'),
            ('apple', 'apple'), ('macbook', 'apple'),
            ('hp ', 'hp'), ('hp-', 'hp'),
            ('lg ', 'lg'), ('lg-', 'lg'),
            ('samsung', 'samsung'), ('gigabyte', 'gigabyte'),
            ('colorful', 'colorful'), ('masstel', 'masstel'),
        ]
        for tok, mfr in _mfr:
            if tok in f' {title_l} ':
                out['Manufacturer'] = mfr
                break

    # ---- CPU ----
    cpu_text = ((_find_spec('cpu', 'bộ xử lý', 'vi xử lý', 'processor') or '') + ' ' + title_l).lower()

    if re.search(r'\bintel\b', cpu_text):
        out['CPU manufacturer'] = 'Intel'
    elif re.search(r'\bamd\b|\bryzen\b', cpu_text):
        out['CPU manufacturer'] = 'AMD'
    elif re.search(r'\bapple\b|\bm[1-4]\b', cpu_text):
        out['CPU manufacturer'] = 'Apple'
    elif re.search(r'\bqualcomm\b|\bsnapdragon\b', cpu_text):
        out['CPU manufacturer'] = 'Qualcomm'
    elif re.search(r'\bi[3579]-?\d{4}', cpu_text):
        # Infer Intel from model number pattern (i5-13420H etc.)
        out['CPU manufacturer'] = 'Intel'

    for pat in [
        r'(core\s+ultra\s*\d+)', r'(ryzen\s*(?:ai\s*)?\d+)',
        r'(i[3579]-?\d{4,5}[a-z]*)', r'(core\s+i[3579])',
        r'(m[1-4]\s*(?:pro|max|ultra)?)',
        r'(pentium|celeron|athlon)', r'(snapdragon\s*\w+)',
    ]:
        m = re.search(pat, cpu_text, re.I)
        if m:
            out['CPU brand modifier'] = m.group(1).strip()
            break

    gen = re.search(r'i[3579]-?(\d{2})\d{2,3}', cpu_text)
    if gen:
        out['CPU generation'] = gen.group(1)
    else:
        ry = re.search(r'ryzen\s*(?:ai\s*)?(\d)', cpu_text, re.I)
        if ry:
            out['CPU generation'] = ry.group(1)
        else:
            ul = re.search(r'ultra\s*(\d)', cpu_text, re.I)
            if ul:
                out['CPU generation'] = ul.group(1)

    spd = re.search(r'(\d+(?:\.\d+)?)\s*ghz', cpu_text, re.I)
    if spd:
        out['CPU Speed (GHz)'] = spd.group(1)

    # ---- RAM (search RAM-specific spec keys first) ----
    ram_text = _find_spec('ram', 'bộ nhớ', 'memory')
    ram_src = ram_text or title or ''
    multi = re.search(r'(\d+)\s*[x×]\s*(\d+)\s*gb', ram_src, re.I)
    if multi:
        out['RAM (GB)'] = str(int(multi.group(1)) * int(multi.group(2)))
    else:
        rm = re.search(r'(\d+)\s*gb', ram_src, re.I)
        if rm and int(rm.group(1)) <= 128:
            out['RAM (GB)'] = rm.group(1)

    rt = re.search(r'(lpddr\s*[45x]|ddr\s*[2345])', f'{ram_src} {title}', re.I)
    if rt:
        out['RAM Type'] = re.sub(r'\s+', '', rt.group(1)).upper()

    bus_src = (_find_spec('bus', 'tốc độ ram', 'ram speed') or '') + ' ' + (ram_text or '')
    bm = re.search(r'(\d{3,4})\s*mhz', bus_src, re.I)
    if bm:
        out['Bus (MHz)'] = bm.group(1)

    # ---- Storage (search storage-specific spec keys first) ----
    stor_text = _find_spec('ssd', 'ổ cứng', 'hard drive', 'storage', 'ổ lưu trữ', 'lưu trữ', 'hdd')
    stor_src = stor_text or title or ''
    tb = re.search(r'(\d+(?:\.\d+)?)\s*tb', stor_src, re.I)
    if tb:
        out['Storage (GB)'] = str(int(float(tb.group(1)) * 1000))
    else:
        # When falling back to title, find ALL GB matches and pick the one
        # that looks like storage (>= 128GB).
        for sg in re.finditer(r'(\d+)\s*gb', stor_src, re.I):
            v = int(sg.group(1))
            if v >= 128:
                out['Storage (GB)'] = str(v)
                break

    # ---- Screen ----
    scr_text = _find_spec('màn hình', 'kích thước màn', 'screen', 'display')
    scr_src = f'{scr_text or ""} {title}'
    sz = re.search(r'(\d+(?:\.\d+)?)\s*(?:inch|"|\'\')', scr_src, re.I)
    if sz:
        out['Screen Size (inch)'] = sz.group(1)

    res_src = (_find_spec('độ phân giải', 'resolution') or '') + ' ' + (scr_text or '')
    rm2 = re.search(r'(\d{3,4})\s*[x×]\s*(\d{3,4})', res_src)
    if rm2:
        out['Screen Resolution'] = f'{rm2.group(1)} x {rm2.group(2)}'

    rr_src = (_find_spec('tần số', 'refresh', 'tần số quét') or '') + ' ' + (scr_text or '')
    rr = re.search(r'(\d{2,3})\s*hz', rr_src, re.I)
    if rr:
        out['Refresh Rate (Hz)'] = rr.group(1)

    # ---- GPU ----
    gpu_text = _find_spec('card đồ họa', 'gpu', 'vga', 'card màn hình', 'đồ họa', 'graphics')
    gpu_src = f'{gpu_text or ""} {title}'.lower()
    if re.search(r'\bnvidia\b|geforce|rtx\s*\d|gtx\s*\d', gpu_src):
        out['GPU manufacturer'] = 'NVIDIA'
    elif re.search(r'radeon\s*rx|rx\s*\d{4}', gpu_src):
        out['GPU manufacturer'] = 'AMD'
    elif out.get('CPU manufacturer') == 'Apple':
        out['GPU manufacturer'] = 'Apple'
    elif re.search(r'intel.*(uhd|iris|xe|arc|graphics)', gpu_src):
        out['GPU manufacturer'] = 'Intel'
    elif re.search(r'radeon|amd.*graphics', gpu_src):
        out['GPU manufacturer'] = 'AMD'

    # ---- Weight ----
    w_text = _find_spec('trọng lượng', 'cân nặng', 'weight', 'khối lượng', 'nặng')
    w_src = w_text or title or ''
    wm = re.search(r'(\d+(?:\.\d+)?)\s*kg', w_src, re.I)
    if wm:
        wt = float(wm.group(1))
        if 0.5 <= wt <= 6.0:
            out['Weight (kg)'] = wm.group(1)

    # ---- Battery (normalise to Wh) ----
    bat_text = _find_spec('pin', 'battery', 'dung lượng pin')
    bat_src = f'{bat_text or ""} {title}'
    wh = re.search(r'(\d+(?:\.\d+)?)\s*wh', bat_src, re.I)
    if wh:
        out['Battery'] = f'{wh.group(1)} Wh'
    else:
        mah = re.search(r'(\d+)\s*mah', bat_src, re.I)
        if mah:
            out['Battery'] = f'{mah.group(1)} mAh'

    # ---- Price ----
    if price_value is not None:
        out['Price (VND)'] = str(price_value)

    return out


def crawl_category(category_url: str, max_pages: int = MAX_PAGES) -> List[Dict]:
    session = create_session()
    base = requests.utils.urlparse(category_url).scheme + '://' + requests.utils.urlparse(category_url).netloc

    product_urls: List[str] = []
    for page in range(1, max_pages + 1):
        url = f"{category_url}?page={page}"
        try:
            r = session.get(url, timeout=15)
            r.raise_for_status()
            urls = parse_category_page(r.text, base)
            if not urls:
                break
            product_urls.extend(urls)
            time.sleep(0.3)
        except Exception:
            break

    # deduplicate while preserving order
    seen = set()
    product_urls = [x for x in product_urls if not (x in seen or seen.add(x))]

    results: List[Dict] = []

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(session.get, u, timeout=15): u for u in product_urls}
        for fut in as_completed(futures):
            u = futures[fut]
            try:
                r = fut.result()
                if r.status_code == 200:
                    # quick laptop page check: look for 'laptop' in title or breadcrumbs
                    html = r.text
                    if 'laptop' not in html.lower() and 'máy tính xách tay' not in html.lower():
                        # skip non-laptop pages
                        continue
                    parsed = parse_product_page(html, u)
                    results.append(parsed)
                time.sleep(0.1)
            except Exception:
                continue

    return results


def main():
    os.makedirs('data', exist_ok=True)
    items = crawl_category(CATEGORY_URL, MAX_PAGES)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f'Saved {len(items)} items to {OUTPUT_PATH}')


if __name__ == '__main__':
    main()