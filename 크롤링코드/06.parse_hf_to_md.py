import re
import json
from pathlib import Path
from collections import defaultdict

import pandas as pd


# =========================
# 경로 설정
# =========================
HF_RAW_CSV = Path("raw/hf/hf_raw.csv")

OUTPUT_HF_DIR = Path("output_md/hf")
OUTPUT_META_DIR = Path("output_md/meta")

OUTPUT_HF_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_META_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_CSV = OUTPUT_META_DIR / "manifest_hf_only.csv"


# =========================
# 섹션 정의
# =========================
SECTION_PATTERNS = {
    "상품설명": [
        r"^상품설명$",
        r"^상품 안내$",
        r"^상품안내$",
        r"^개요$",
        r"^설명$",
        r"^보증상품 개요$",
    ],
    "보증대상": [
        r"^보증대상$",
        r"^대상$",
        r"^보증 대상$",
        r"^가입대상$",
        r"^이용대상$",
        r"^대상자$",
        r"^보증대상자$",
        r"^대상주택$",
        r"^보증대상 목적물$",
        r"^목적물$",
    ],
    "신청시기": [
        r"^신청시기$",
        r"^보증신청시기$",
        r"^신청기한$",
        r"^신청기간$",
        r"^보증 가입 시기$",
        r"^가입시기$",
    ],
    "제출서류": [
        r"^제출서류$",
        r"^서류제출$",
        r"^필요서류$",
        r"^구비서류$",
        r"^준비서류$",
    ],
    "이용절차": [
        r"^보증이용절차$",
        r"^이용절차$",
        r"^신청절차$",
        r"^절차$",
        r"^가입절차$",
    ],
    "보증료": [
        r"^보증료$",
        r"^보증료율$",
        r"^수수료$",
        r"^요율$",
    ],
    "유의사항": [
        r"^유의사항$",
        r"^기타사항$",
        r"^주의사항$",
        r"^참고사항$",
        r"^안내사항$",
    ],
}

ALL_SECTION_NAMES = list(SECTION_PATTERNS.keys()) + ["기타"]


# =========================
# HF 전용 규칙
# =========================
GENERIC_BAD_NAMES = {
    "HF 한국주택금융공사",
    "한국주택금융공사",
    "HF",
    "주택담보대출",
    "주택보증",
    "주택연금",
    "상품비교",
    "상품소개",
    "상품안내",
    "사용 메뉴얼",
}

KEEP_KEYWORDS = [
    "보증", "대출", "연금", "전세", "월세", "자금"
]

DROP_MAIN_TEXT_HINTS = [
    "검색조건을 선택해주세요",
    "목적물소재지에서 필요자금까지 원하시는 검색조건을 선택해주세요",
    "검색조건을 선택하신후에 조회하기 버튼을 클릭해주세요",
    "비교하기",
    "조회하기",
    "상세확인",
    "최대 3가지 상품을 비교",
    "상품비교",
    "사용 메뉴얼",
]

HF_URL_NAME_MAP = {
    "sub02_01_01": "일반전세지킴보증",
    "sub02_01_02": "특례전세지킴보증",
    "sub02_01_03": "전세자금보증",
    "sub02_01_04": "전세지킴보증 신청가능여부 확인",
    "sub02_01_05": "전세보증금반환보증",
    "sub02_02_01": "일반전세자금보증",
    "sub02_02_02": "특례전세자금보증",
    "sub02_02_03": "협약전세자금보증",
    "sub02_03_01": "주택구입자금보증",
    "sub02_04_01": "개량자금보증",
    "sub02_05_01": "주택사업자보증",
    "sub02_06_01": "중도금보증",
    "sub03_02_05": "주택연금",
}


# =========================
# 유틸
# =========================
def clean_text(text: str) -> str:
    if text is None:
        return ""
    text = str(text).replace("\xa0", " ")
    text = text.replace("\u200b", " ")
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_line(line: str) -> str:
    line = clean_text(line)
    line = re.sub(r"\s+", " ", line)
    return line.strip()


def safe_filename(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    name = re.sub(r"\s+", "_", name).strip("_")
    return name[:180] if name else "document"


def normalize_url(url: str) -> str:
    url = clean_text(url)
    url = re.sub(r"^http://", "https://", url, flags=re.I)
    return url


def load_json_list(value: str) -> list:
    if not value:
        return []
    try:
        return json.loads(value)
    except Exception:
        return []


def dedupe_preserve_order(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        x = clean_text(item)
        if not x:
            continue
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def has_keep_keyword(text: str) -> bool:
    text = clean_text(text)
    return any(k in text for k in KEEP_KEYWORDS)


def looks_generic_name(name: str) -> bool:
    name = clean_text(name)
    return (not name) or (name in GENERIC_BAD_NAMES)


def should_drop_row(main_text: str, title: str, url: str) -> bool:
    joined = " ".join([clean_text(title), clean_text(main_text[:1000]), clean_text(url)])
    if any(hint in joined for hint in DROP_MAIN_TEXT_HINTS):
        return True
    return False


# =========================
# 상품명 추출
# =========================
def infer_from_url(url: str) -> str:
    url = normalize_url(url)
    for k, v in HF_URL_NAME_MAP.items():
        if k in url:
            return v
    return ""


def infer_from_breadcrumb(breadcrumb: str) -> str:
    breadcrumb = clean_text(breadcrumb)
    if not breadcrumb:
        return ""

    parts = [x.strip() for x in re.split(r">|<", breadcrumb) if x.strip()]
    blacklist = {
        "한국주택금융공사",
        "HF",
        "주택담보대출",
        "주택보증",
        "주택연금",
        "한눈에 보는 개인보증상품",
        "내게 맞는 개인보증상품",
    }
    parts = [p for p in parts if p not in blacklist]

    for p in reversed(parts):
        if has_keep_keyword(p) and not looks_generic_name(p):
            return p

    return ""


def infer_from_headings(headings_json: str) -> str:
    headings = load_json_list(headings_json)
    for h in headings:
        h = normalize_line(h)
        if not h:
            continue
        if looks_generic_name(h):
            continue
        if has_keep_keyword(h):
            return h
    return ""


def infer_from_main_text(main_text: str) -> str:
    lines = [normalize_line(x) for x in clean_text(main_text).split("\n")]
    lines = [x for x in lines if x][:60]

    for line in lines:
        if looks_generic_name(line):
            continue
        if len(line) < 4 or len(line) > 40:
            continue
        if has_keep_keyword(line):
            return line

    return ""


def infer_product_name(title: str, breadcrumb: str, url: str, headings_json: str, main_text: str) -> str:
    title = clean_text(title)

    # 1) URL 매핑 우선
    x = infer_from_url(url)
    if x:
        return x

    # 2) breadcrumb
    x = infer_from_breadcrumb(breadcrumb)
    if x:
        return x

    # 3) title
    if title and not looks_generic_name(title) and has_keep_keyword(title):
        return title

    # 4) headings
    x = infer_from_headings(headings_json)
    if x:
        return x

    # 5) 본문 앞부분
    x = infer_from_main_text(main_text)
    if x:
        return x

    return ""


# =========================
# 섹션 분리
# =========================
def match_section_heading(line: str) -> str | None:
    line = normalize_line(line)
    for section_name, patterns in SECTION_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, line):
                return section_name
    return None


def split_into_sections(main_text: str, headings_json: str = "", lists_json: str = "") -> dict:
    lines = [normalize_line(x) for x in clean_text(main_text).split("\n")]
    lines = [x for x in lines if x]

    sections = {k: [] for k in ALL_SECTION_NAMES}
    current_section = "기타"

    for line in lines:
        matched = match_section_heading(line)
        if matched:
            current_section = matched
            continue

        sections[current_section].append(line)

    heading_items = load_json_list(headings_json)
    list_items = load_json_list(lists_json)

    for item in list_items:
        item = clean_text(item)
        if not item:
            continue
        sections["기타"].append(item)

    final_sections = {}
    for key, values in sections.items():
        values = dedupe_preserve_order(values)
        final_sections[key] = "\n".join(values).strip()

    return final_sections


# =========================
# TXT 렌더링
# =========================
def render_document_txt(
    product_name: str,
    url_list: list[str],
    collected_at: str,
    sections: dict,
) -> str:
    parts = []
    parts.append(f"# 상품명: {product_name}")
    parts.append("기관명: HF")
    parts.append(f"원본 URL: {' | '.join(url_list)}")
    parts.append(f"수집일: {collected_at}")
    parts.append("출처유형: WEB")

    for sec_name in ["상품설명", "보증대상", "신청시기", "제출서류", "이용절차", "보증료", "유의사항", "기타"]:
        value = clean_text(sections.get(sec_name, ""))
        if value:
            parts.append(f"\n## {sec_name}\n{value}")

    return "\n".join(parts).strip() + "\n"


# =========================
# HF 기존 txt 삭제
# =========================
def cleanup_old_hf_txt():
    for p in OUTPUT_HF_DIR.glob("HF_*.txt"):
        try:
            p.unlink()
        except Exception:
            pass


# =========================
# 메인
# =========================
def main():
    if not HF_RAW_CSV.exists():
        raise FileNotFoundError(f"파일이 없습니다: {HF_RAW_CSV}")

    print("[1] HF raw CSV 로드")
    df = pd.read_csv(HF_RAW_CSV, dtype=str).fillna("")
    print(f" - 입력 행 수: {len(df)}")

    print("[2] 상품명 추출 및 불필요 행 제거")
    rows = []

    for _, row in df.iterrows():
        title = clean_text(row.get("title", ""))
        url = normalize_url(row.get("url", ""))
        breadcrumb = clean_text(row.get("breadcrumb", ""))
        headings_json = row.get("headings_json", "")
        lists_json = row.get("lists_json", "")
        main_text = clean_text(row.get("main_text", ""))
        collected_at = clean_text(row.get("collected_at", "")) or pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

        if not main_text:
            continue

        if should_drop_row(main_text, title, url):
            continue

        product_name = infer_product_name(title, breadcrumb, url, headings_json, main_text)
        if not product_name:
            continue

        sections = split_into_sections(main_text, headings_json, lists_json)

        rows.append({
            "product_name": product_name,
            "url": url,
            "collected_at": collected_at,
            "sections": sections,
        })

    print(f" - 유지 행 수: {len(rows)}")

    print("[3] 상품명 기준 병합")
    grouped = defaultdict(list)
    for r in rows:
        grouped[r["product_name"]].append(r)

    merged_docs = []
    for product_name, items in grouped.items():
        merged_sections = {k: [] for k in ALL_SECTION_NAMES}
        urls = []
        collected_ats = []

        for item in items:
            urls.append(item["url"])
            collected_ats.append(item["collected_at"])
            for sec_name in ALL_SECTION_NAMES:
                val = clean_text(item["sections"].get(sec_name, ""))
                if val:
                    merged_sections[sec_name].append(val)

        final_sections = {}
        for sec_name, values in merged_sections.items():
            values = dedupe_preserve_order(values)
            final_sections[sec_name] = "\n\n".join(values).strip()

        merged_docs.append({
            "product_name": product_name,
            "urls": dedupe_preserve_order(urls),
            "collected_at": max(collected_ats) if collected_ats else "",
            "sections": final_sections,
        })

    print(f" - 최종 문서 수: {len(merged_docs)}")

    print("[4] 기존 HF txt 삭제")
    cleanup_old_hf_txt()

    print("[5] 새 HF txt 저장")
    manifest_rows = []

    for doc in merged_docs:
        product_name = doc["product_name"]
        urls = doc["urls"]
        collected_at = doc["collected_at"]
        sections = doc["sections"]

        file_name = safe_filename(f"HF_{product_name}__WEB.txt")
        out_path = OUTPUT_HF_DIR / file_name

        txt = render_document_txt(
            product_name=product_name,
            url_list=urls,
            collected_at=collected_at,
            sections=sections,
        )
        out_path.write_text(txt, encoding="utf-8")

        manifest_rows.append({
            "source_org": "HF",
            "source_type": "WEB",
            "product_name": product_name,
            "file_name": file_name,
            "file_path": str(out_path),
            "url": " | ".join(urls),
            "collected_at": collected_at,
            "text_len": len(txt),
        })

    manifest_df = pd.DataFrame(manifest_rows).sort_values(["product_name", "file_name"])
    manifest_df.to_csv(MANIFEST_CSV, index=False, encoding="utf-8-sig")

    print("완료")
    print(f"- HF 출력 폴더: {OUTPUT_HF_DIR}")
    print(f"- HF manifest : {MANIFEST_CSV}")


if __name__ == "__main__":
    main()