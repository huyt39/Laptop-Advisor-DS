from __future__ import annotations

import re
from typing import Dict, Optional


def extract_features(title: str, specs: Dict[str, str], price_value: Optional[int]) -> Dict[str, Optional[str]]:
    """Extract normalized laptop features from a product title and raw spec table."""
    fields = [
        "Manufacturer",
        "CPU manufacturer",
        "CPU brand modifier",
        "CPU generation",
        "CPU Speed (GHz)",
        "RAM (GB)",
        "RAM Type",
        "Bus (MHz)",
        "Storage (GB)",
        "Screen Size (inch)",
        "Screen Resolution",
        "Refresh Rate (Hz)",
        "GPU manufacturer",
        "GPU model",
        "GPU type",
        "Weight (kg)",
        "Battery",
        "Battery (Wh)",
        "Battery life (hours)",
        "Price (VND)",
    ]
    out: Dict[str, Optional[str]] = {k: None for k in fields}
    title_l = (title or "").lower()
    all_spec_text = " ".join(str(v) for v in specs.values() if v)

    def _find_spec(*keywords: str) -> Optional[str]:
        for k, v in specs.items():
            kl = k.lower()
            for kw in keywords:
                if kw in kl and v and v.strip():
                    return v.strip()
        return None

    brand_spec = _find_spec("hãng", "thương hiệu", "brand", "manufacturer")
    if brand_spec:
        out["Manufacturer"] = brand_spec
    else:
        manufacturers = [
            ("asus", "asus"),
            ("lenovo", "lenovo"),
            ("dell", "dell"),
            ("acer", "acer"),
            ("msi ", "msi"),
            ("msi-", "msi"),
            ("apple", "apple"),
            ("macbook", "apple"),
            ("hp ", "hp"),
            ("hp-", "hp"),
            ("lg ", "lg"),
            ("lg-", "lg"),
            ("samsung", "samsung"),
            ("gigabyte", "gigabyte"),
            ("colorful", "colorful"),
            ("masstel", "masstel"),
        ]
        for token, manufacturer in manufacturers:
            if token in f" {title_l} ":
                out["Manufacturer"] = manufacturer
                break

    cpu_text = ((_find_spec("cpu", "bộ xử lý", "vi xử lý", "processor") or "") + " " + title_l).lower()
    if re.search(r"\bintel\b", cpu_text):
        out["CPU manufacturer"] = "Intel"
    elif re.search(r"\bamd\b|\bryzen\b", cpu_text):
        out["CPU manufacturer"] = "AMD"
    elif re.search(r"\bqualcomm\b|\bsnapdragon\b|\bx1p?[-\s]\d", cpu_text):
        out["CPU manufacturer"] = "Qualcomm"
    elif re.search(r"\bi[3579]-?\d{4}|\bcore\s+(?:ultra\s*)?[3579]\b|\bultra\s*[3579]\b", cpu_text):
        out["CPU manufacturer"] = "Intel"
    elif re.search(r"\bapple\b|\bmacbook\b|\bm[1-5]\b", cpu_text):
        out["CPU manufacturer"] = "Apple"

    for pattern in [
        r"(core\s+ultra\s*\d+)",
        r"(ultra\s*\d+)",
        r"(core\s*[3579])",
        r"(ryzen\s*(?:ai\s*)?\d+)",
        r"(i[3579]-?\d{4,5}[a-z]*)",
        r"(core\s+i[3579])",
        r"(m[1-5]\s*(?:pro|max|ultra)?)",
        r"(?:apple\s+)(a\d{2,3}\s*(?:pro|max|ultra)?)",
        r"(pentium|celeron|athlon)",
        r"(snapdragon\s*\w+)",
        r"(x1p?[-\s]\d[\w-]*)",
    ]:
        match = re.search(pattern, cpu_text, re.I)
        if match:
            out["CPU brand modifier"] = match.group(1).strip()
            break

    gen = re.search(r"i[3579]-?(\d{2})\d{2,3}", cpu_text)
    if gen:
        out["CPU generation"] = gen.group(1)
    else:
        ryzen = re.search(r"ryzen\s*(?:ai\s*)?(\d)", cpu_text, re.I)
        ultra = re.search(r"ultra\s*(\d)", cpu_text, re.I)
        if ryzen:
            out["CPU generation"] = ryzen.group(1)
        elif ultra:
            out["CPU generation"] = ultra.group(1)

    speed = re.search(r"(\d+(?:\.\d+)?)\s*ghz", cpu_text, re.I)
    if speed:
        out["CPU Speed (GHz)"] = speed.group(1)

    ram_text = _find_spec("ram", "bộ nhớ", "memory", "tên sku", "thông số nổi bật")
    ram_src = f"{ram_text or ''} {title} {all_spec_text}"
    multi = re.search(r"(\d+)\s*[x×]\s*(\d+)\s*gb", ram_src, re.I)
    if multi:
        out["RAM (GB)"] = str(int(multi.group(1)) * int(multi.group(2)))
    else:
        for ram in re.finditer(r"(\d+)\s*gb", ram_src, re.I):
            value = int(ram.group(1))
            if value <= 128:
                out["RAM (GB)"] = ram.group(1)
                break

    ram_type = re.search(r"(lpddr\s*[45x]|ddr\s*[2345])", f"{ram_src} {title}", re.I)
    if ram_type:
        out["RAM Type"] = re.sub(r"\s+", "", ram_type.group(1)).upper()

    bus_src = (_find_spec("bus", "tốc độ ram", "ram speed") or "") + " " + (ram_text or "")
    bus = re.search(r"(\d{3,4})\s*mhz", bus_src, re.I)
    if bus:
        out["Bus (MHz)"] = bus.group(1)

    storage_text = _find_spec("ssd", "ổ cứng", "hard drive", "storage", "ổ lưu trữ", "lưu trữ", "hdd", "tên sku")
    storage_src = f"{storage_text or ''} {title} {all_spec_text}"
    for tb in re.finditer(r"(\d+(?:\.\d+)?)\s*(?:tb|t\b)", storage_src, re.I):
        value_tb = float(tb.group(1))
        if 0.1 <= value_tb <= 8:
            out["Storage (GB)"] = str(int(value_tb * 1000))
            break
    if not out["Storage (GB)"]:
        for storage in re.finditer(r"(\d+)\s*(?:ssd|s)?\s*gb", storage_src, re.I):
            value = int(storage.group(1))
            if 128 <= value <= 8192:
                out["Storage (GB)"] = str(value)
                break

    screen_text = _find_spec("màn hình", "kích thước màn", "screen", "display")
    screen_src = f"{screen_text or ''} {title}"
    size = re.search(r"(\d+(?:\.\d+)?)\s*(?:inch|\"|'')", screen_src, re.I)
    if size:
        out["Screen Size (inch)"] = size.group(1)

    resolution_src = (_find_spec("độ phân giải", "resolution") or "") + " " + (screen_text or "")
    resolution = re.search(r"(\d{3,4})\s*[x×]\s*(\d{3,4})", resolution_src)
    if resolution:
        out["Screen Resolution"] = f"{resolution.group(1)} x {resolution.group(2)}"

    refresh_src = (_find_spec("tần số", "refresh", "tần số quét") or "") + " " + (screen_text or "")
    refresh = re.search(r"(\d{2,3})\s*hz", refresh_src, re.I)
    if refresh:
        out["Refresh Rate (Hz)"] = refresh.group(1)

    gpu_text = _find_spec("card đồ họa", "card đồ hoạ", "gpu", "vga", "card màn hình", "đồ họa", "đồ hoạ", "graphics")
    gpu_src = f"{gpu_text or ''} {title} {all_spec_text}".lower()
    if re.search(r"\bnvidia\b|geforce|rtx\s*\d|gtx\s*\d", gpu_src):
        out["GPU manufacturer"] = "NVIDIA"
    elif re.search(r"radeon\s*rx|rx\s*\d{4}", gpu_src):
        out["GPU manufacturer"] = "AMD"
    elif re.search(r"intel.*(uhd|iris|xe|arc|graphics)", gpu_src):
        out["GPU manufacturer"] = "Intel"
    elif re.search(r"radeon|amd.*graphics", gpu_src):
        out["GPU manufacturer"] = "AMD"
    elif re.search(r"qualcomm|adreno", gpu_src):
        out["GPU manufacturer"] = "Qualcomm"
    elif out.get("CPU manufacturer") == "Apple":
        out["GPU manufacturer"] = "Apple"
    elif out.get("CPU manufacturer") == "Intel":
        out["GPU manufacturer"] = "Intel"
    elif out.get("CPU manufacturer") == "AMD":
        out["GPU manufacturer"] = "AMD"

    gpu_model_patterns = [
        (r"\b(?:nvidia\s+geforce\s+)?(rtx\s*\d{3,4}\s*ti)\b", "RTX {number} Ti"),
        (r"\b(?:nvidia\s+geforce\s+)?(rtx\s*\d{3,4})\b", "RTX {number}"),
        (r"\b(?:nvidia\s+geforce\s+)?(gtx\s*\d{3,4}\s*ti)\b", "GTX {number} Ti"),
        (r"\b(?:nvidia\s+geforce\s+)?(gtx\s*\d{3,4})\b", "GTX {number}"),
        (r"\b(?:nvidia\s+geforce\s+)?(mx\s*\d{3,4}[a-z]?)\b", "MX{number}"),
        (r"\b(?:amd\s+)?radeon\s+(rx\s*\d{3,4}[a-z]*)\b", "Radeon RX {number}"),
        (r"\b(?:amd\s+)?radeon\s+(\d{3,4}[a-z]*m?)\s*(?:graphics)?\b", "Radeon {number}"),
    ]
    for pattern, template in gpu_model_patterns:
        match = re.search(pattern, gpu_src, re.I)
        if not match:
            continue
        token = match.group(1)
        number = re.sub(r"(?i)^(?:rtx|gtx|mx|rx)\s*", "", token)
        number = re.sub(r"(?i)\s*ti$", "", number).upper()
        out["GPU model"] = template.format(number=number)
        break

    if not out["GPU model"]:
        if re.search(r"\bintel\s+arc(?:\s+graphics)?\b", gpu_src):
            out["GPU model"] = "Intel Arc Graphics"
        elif re.search(r"\bintel\s+iris\s+xe(?:\s+graphics)?\b", gpu_src):
            out["GPU model"] = "Intel Iris Xe"
        elif re.search(r"\bintel\s+uhd(?:\s+graphics)?\b", gpu_src):
            out["GPU model"] = "Intel UHD"
        elif re.search(r"\bintel\s+graphics\b", gpu_src):
            out["GPU model"] = "Intel Graphics"
        elif re.search(r"\b(?:amd\s+)?radeon\s+graphics\b", gpu_src):
            out["GPU model"] = "AMD Radeon Graphics"
        elif re.search(r"\bqualcomm\s+adreno(?:\s+gpu)?\b|\badreno\b", gpu_src):
            out["GPU model"] = "Qualcomm Adreno"
        elif out.get("GPU manufacturer") == "Apple":
            chip = re.search(r"\b(m[1-5](?:\s+(?:pro|max|ultra))?|a\d{2,3}\s*pro)\b", gpu_src, re.I)
            cores = re.search(r"\b(\d{1,2})\s*gpu\b", gpu_src, re.I)
            chip_name = chip.group(1).upper() if chip else ""
            if chip_name and cores:
                out["GPU model"] = f"Apple {chip_name} {cores.group(1)}-core GPU"
            elif chip_name:
                out["GPU model"] = f"Apple {chip_name} GPU"
            else:
                out["GPU model"] = "Apple GPU"
        elif out.get("GPU manufacturer") == "Intel":
            out["GPU model"] = "Intel Graphics"
        elif out.get("GPU manufacturer") == "AMD":
            out["GPU model"] = "AMD Radeon Graphics"
        elif out.get("GPU manufacturer") == "Qualcomm":
            out["GPU model"] = "Qualcomm Adreno"

    if out.get("GPU manufacturer") == "NVIDIA" or re.search(r"\bradeon\s+rx\b", gpu_src):
        out["GPU type"] = "Dedicated"
    elif out.get("GPU manufacturer") in {"Intel", "AMD", "Apple", "Qualcomm"}:
        out["GPU type"] = "Integrated"

    weight_text = _find_spec("trọng lượng", "cân nặng", "weight", "khối lượng", "nặng")
    weight_src = f"{weight_text or ''} {all_spec_text} {title}"
    weight = re.search(r"(\d+(?:\.\d+)?)\s*kg", weight_src, re.I)
    if weight:
        parsed = float(weight.group(1))
        if 0.5 <= parsed <= 6.0:
            out["Weight (kg)"] = weight.group(1)

    battery_text = _find_spec("pin", "battery", "dung lượng pin")
    battery_src = f"{battery_text or ''} {all_spec_text} {title}"
    wh = re.search(r"(\d+(?:\.\d+)?)\s*wh(?:r)?\b", battery_src, re.I)
    if wh:
        out["Battery"] = f"{wh.group(1)} Wh"
        out["Battery (Wh)"] = wh.group(1)
    else:
        mah = re.search(r"(\d+)\s*mah", battery_src, re.I)
        if mah:
            out["Battery"] = f"{mah.group(1)} mAh"

    hours = re.search(
        r"(?:thời lượng\s+pin|pin(?:\s+dài|\s+lên)?(?:\s+đến)?)[^0-9]{0,20}(\d+(?:\.\d+)?)\s*(?:giờ|tiếng|h\b)",
        battery_src,
        re.I,
    )
    if hours:
        out["Battery life (hours)"] = hours.group(1)
        if not out["Battery"]:
            out["Battery"] = f"{hours.group(1)} hours"

    if price_value is not None:
        out["Price (VND)"] = str(price_value)

    return out
