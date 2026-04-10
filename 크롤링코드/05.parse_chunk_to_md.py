import re
import json
from pathlib import Path
from collections import defaultdict

import pandas as pd


# =========================
# 입력 경로
# =========================
HF_RAW_CSV = Path("raw/hf/hf_raw.csv")
HUG_RAW_CSV = Path("raw/hug/hug_raw.csv")
HUG_PDF_CSV = Path("raw/hug/hug_pdf_sections.csv")

# =========================
# 출력 경로
# =========================
OUTPUT_HF_DIR = Path("output_md/hf")
OUTPUT_HUG_DIR = Path("output_md/hug")
OUTPUT_META_DIR = Path("output_md/meta")

OUTPUT_HF_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_HUG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_META_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_CSV = OUTPUT_META_DIR / "manifest.csv"


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
        r"^언제 신청.*$",
    ],
    "제출서류": [
        r"^제출서류$",
        r"^서류제출$",
        r"^필요서류$",
        r"^구비서류$",
        r"^준비서류$",
        r"^제출 서류 안내$",
    ],
    "이용절차": [
        r"^보증이용절차$",
        r"^이용절차$",
        r"^신청절차$",
        r"^절차$",
        r"^신청절차 안내$",
        r"^가입절차$",
    ],
    "보증료": [
        r"^보증료$",
        r"^보증료율$",
        r"^수수료$",
        r"^요율$",
        r"^보증료 안내$",
    ],
    "유의사항": [
        r"^유의사항$",
        r"^기타사항$",
        r"^보증료 외 기타사항 안내$",
        r"^기타 안내$",
        r"^주의사항$",
        r"^참고사항$",
        r"^안내사항$",
    ],
}

ALL_SECTION_NAMES = list(SECTION_PATTERNS.keys()) + ["기타"]


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


def infer_product_name(title: str, breadcrumb: str, url: str, source_type: str = "") -> str:
    title = clean_text(title)
    breadcrumb = clean_text(breadcrumb)
    url = clean_text(url)

    bad_titles = {
        "HF 한국주택금융공사",
        "한국주택금융공사",
        "HF",
        "주택도시보증공사",
        "HUG",
    }

    # 1) title 우선, 단 공통 제목은 제외
    if title and title not in bad_titles:
        title = re.sub(r"\s*-\s*한국주택금융공사.*$", "", title)
        title = re.sub(r"\s*-\s*주택도시보증공사.*$", "", title)
        title = re.sub(r"\s*\|\s*한국주택금융공사.*$", "", title)
        title = re.sub(r"\s*\|\s*주택도시보증공사.*$", "", title)

        # HUG 스타일 "상품개요 < 전세보증금반환보증 < 개인보증 < 주택도시보증공사"
        if "<" in title:
            parts = [x.strip() for x in title.split("<") if x.strip()]
            for p in parts:
                if p not in ["상품개요", "개인보증", "기업보증", "주택도시보증공사"]:
                    return p

        return title.strip()

    # 2) breadcrumb에서 가장 구체적인 상품명 찾기
    if breadcrumb:
        parts = [x.strip() for x in re.split(r">|<", breadcrumb) if x.strip()]
        blacklist = {
            "한국주택금융공사",
            "주택도시보증공사",
            "HF",
            "HUG",
            "개인보증",
            "기업보증",
            "주택보증",
            "주택담보대출",
            "주택연금",
            "한눈에 보는 개인보증상품",
            "내게 맞는 개인보증상품",
        }
        parts = [p for p in parts if p not in blacklist]

        # 뒤에서부터 상품명 후보 찾기
        for p in reversed(parts):
            if any(k in p for k in ["보증", "대출", "연금", "전세", "월세", "자금"]):
                return p

        if parts:
            return parts[-1]

    # 3) URL 패턴 기반 fallback
    if source_type.startswith("PDF"):
        return "HUG 업무지침"

    if url:
        # HF 세부 URL 매핑
        hf_map = {
            "sub02_01_01": "일반전세지킴보증",
            "sub02_01_05": "전세보증금반환보증",
        }
        for k, v in hf_map.items():
            if k in url:
                return v

        # HUG/HF 공통 fallback
        m = re.search(r"/([^/]+)\.(do|jsp)$", url)
        if m:
            return m.group(1)

    return "미분류상품"


def match_section_heading(line: str) -> str | None:
    line = normalize_line(line)
    for section_name, patterns in SECTION_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, line):
                return section_name
    return None


# =========================
# 본문 섹션 분해
# =========================
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

    # headings/lists 보강
    heading_items = load_json_list(headings_json)
    list_items = load_json_list(lists_json)

    for item in list_items:
        item = clean_text(item)
        if not item:
            continue

        guessed = None
        joined_headings = " ".join([clean_text(h) for h in heading_items])

        if any(k in joined_headings for k in ["제출서류", "서류제출", "구비서류", "필요서류"]):
            guessed = "제출서류"
        elif any(k in joined_headings for k in ["이용절차", "절차", "신청절차"]):
            guessed = "이용절차"

        if guessed:
            sections[guessed].append(item)
        else:
            sections["기타"].append(item)

    # 정리
    final_sections = {}
    for key, values in sections.items():
        values = dedupe_preserve_order(values)
        final_sections[key] = "\n".join(values).strip()

    return final_sections


# =========================
# TXT 렌더링
# =========================
def render_document_txt(
    source_org: str,
    product_name: str,
    url: str,
    collected_at: str,
    source_type: str,
    sections: dict,
) -> str:
    parts = []

    parts.append(f"# 상품명: {product_name}")
    parts.append(f"기관명: {source_org}")
    parts.append(f"원본 URL: {url if url else 'PDF 내부 문서'}")
    parts.append(f"수집일: {collected_at}")
    parts.append(f"출처유형: {source_type}")

    for sec_name in ["상품설명", "보증대상", "신청시기", "제출서류", "이용절차", "보증료", "유의사항", "기타"]:
        value = clean_text(sections.get(sec_name, ""))
        if value:
            parts.append(f"\n## {sec_name}\n{value}")

    return "\n".join(parts).strip() + "\n"


# =========================
# 레코드 로드
# =========================
def load_input_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def build_records_from_df(df: pd.DataFrame, default_source_type: str) -> list[dict]:
    records = []

    if df.empty:
        return records

    for _, row in df.iterrows():
        source_org = clean_text(row.get("source_org", ""))
        title = clean_text(row.get("title", ""))
        url = clean_text(row.get("url", ""))
        breadcrumb = clean_text(row.get("breadcrumb", ""))
        main_text = clean_text(row.get("main_text", ""))
        headings_json = row.get("headings_json", "")
        lists_json = row.get("lists_json", "")
        collected_at = clean_text(row.get("collected_at", "")) or pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        source_type = clean_text(row.get("source_type", "")) or default_source_type

        if not main_text:
            continue

        product_name = infer_product_name(title, breadcrumb, url, source_type)
        sections = split_into_sections(main_text, headings_json, lists_json)

        records.append({
            "source_org": source_org,
            "source_type": source_type,
            "product_name": product_name,
            "title": title,
            "url": url,
            "breadcrumb": breadcrumb,
            "collected_at": collected_at,
            "sections": sections,
            "main_text": main_text,
        })

    return records


# =========================
# 병합 로직
# =========================
def merge_same_product_records(records: list[dict]) -> list[dict]:
    """
    같은 기관 + 같은 상품명 + 같은 source_type 기준으로 병합
    웹/웹끼리, PDF/PDF끼리 합침
    WEB과 PDF는 분리 유지
    """
    grouped = defaultdict(list)
    for rec in records:
        key = (rec["source_org"], rec["product_name"], rec["source_type"])
        grouped[key].append(rec)

    merged = []

    for (source_org, product_name, source_type), items in grouped.items():
        merged_sections = {k: [] for k in ALL_SECTION_NAMES}
        urls = []
        collected_ats = []
        titles = []
        breadcrumbs = []

        for item in items:
            titles.append(item.get("title", ""))
            breadcrumbs.append(item.get("breadcrumb", ""))
            if item.get("url"):
                urls.append(item["url"])
            if item.get("collected_at"):
                collected_ats.append(item["collected_at"])

            for sec_name in ALL_SECTION_NAMES:
                val = clean_text(item["sections"].get(sec_name, ""))
                if val:
                    merged_sections[sec_name].append(val)

        final_sections = {}
        for sec_name, values in merged_sections.items():
            values = dedupe_preserve_order(values)
            final_sections[sec_name] = "\n\n".join(values).strip()

        merged.append({
            "source_org": source_org,
            "source_type": source_type,
            "product_name": product_name,
            "title": " | ".join(dedupe_preserve_order(titles)),
            "url": " | ".join(dedupe_preserve_order(urls)),
            "breadcrumb": " | ".join(dedupe_preserve_order(breadcrumbs)),
            "collected_at": max(collected_ats) if collected_ats else "",
            "sections": final_sections,
        })

    return merged


# =========================
# 저장
# =========================
def save_txt_documents(records: list[dict]):
    manifest_rows = []

    for rec in records:
        source_org = rec["source_org"]
        source_type = rec["source_type"]
        product_name = rec["product_name"]
        url = rec["url"]
        collected_at = rec["collected_at"]
        sections = rec["sections"]

        # WEB / PDF 구분 유지
        suffix = "__PDF" if "PDF" in source_type else "__WEB"

        if source_org.upper() == "HF":
            out_dir = OUTPUT_HF_DIR
            file_name = f"HF_{product_name}{suffix}.txt"
        else:
            out_dir = OUTPUT_HUG_DIR
            file_name = f"HUG_{product_name}{suffix}.txt"

        file_name = safe_filename(file_name)
        out_path = out_dir / file_name

        txt = render_document_txt(
            source_org=source_org,
            product_name=product_name,
            url=url,
            collected_at=collected_at,
            source_type=source_type,
            sections=sections,
        )
        out_path.write_text(txt, encoding="utf-8")

        manifest_rows.append({
            "source_org": source_org,
            "source_type": source_type,
            "product_name": product_name,
            "file_name": file_name,
            "file_path": str(out_path),
            "url": url,
            "collected_at": collected_at,
            "text_len": len(txt),
        })

    manifest_df = pd.DataFrame(manifest_rows).sort_values(["source_org", "product_name", "file_name"])
    manifest_df.to_csv(MANIFEST_CSV, index=False, encoding="utf-8-sig")
    return manifest_df


# =========================
# 메인
# =========================
def main():
    print("[1] 입력 CSV 로드")
    hf_df = load_input_csv(HF_RAW_CSV)
    hug_df = load_input_csv(HUG_RAW_CSV)
    hug_pdf_df = load_input_csv(HUG_PDF_CSV)

    print(f" - HF 웹 행 수     : {len(hf_df)}")
    print(f" - HUG 웹 행 수    : {len(hug_df)}")
    print(f" - HUG PDF 행 수   : {len(hug_pdf_df)}")

    print("[2] 공통 레코드 생성")
    hf_records = build_records_from_df(hf_df, default_source_type="WEB")
    hug_records = build_records_from_df(hug_df, default_source_type="WEB")
    hug_pdf_records = build_records_from_df(hug_pdf_df, default_source_type="PDF_SECTION")

    all_records = hf_records + hug_records + hug_pdf_records
    print(f" - 전체 레코드 수  : {len(all_records)}")

    print("[3] 상품 단위 병합")
    merged_records = merge_same_product_records(all_records)
    print(f" - 병합 후 문서 수 : {len(merged_records)}")

    print("[4] TXT 문서 저장")
    manifest_df = save_txt_documents(merged_records)
    print(f" - 생성 파일 수    : {len(manifest_df)}")
    print(f" - manifest 저장   : {MANIFEST_CSV}")

    print("완료")
    print(f"- HF 출력 폴더  : {OUTPUT_HF_DIR}")
    print(f"- HUG 출력 폴더 : {OUTPUT_HUG_DIR}")


if __name__ == "__main__":
    main()