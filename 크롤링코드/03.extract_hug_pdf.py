import re
import json
from pathlib import Path
from collections import OrderedDict

import pandas as pd
from pypdf import PdfReader


# =========================
# 경로 설정
# =========================
PDF_PATH = Path("khug_2025.pdf")

RAW_DIR = Path("raw/hug")
RAW_DIR.mkdir(parents=True, exist_ok=True)

RAW_TEXT_DIR = Path("raw_text/hug")
RAW_TEXT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PAGES_CSV = RAW_DIR / "hug_pdf_pages.csv"
OUTPUT_SECTIONS_CSV = RAW_DIR / "hug_pdf_sections.csv"
OUTPUT_SECTIONS_JSONL = RAW_DIR / "hug_pdf_sections.jsonl"
OUTPUT_FULL_TXT = RAW_TEXT_DIR / "HUG_업무지침_2025_08.txt"


#
'''
지금은 2026년 4월 2일 목요일 입니다. 시간은 오전 10시 50분을 조금 지나고 있네요. 오늘도 어김없이 뜬금없는 정보나 뉴스들 좀 보여주세요.
'''
#



# =========================
# 사용자 조정 가능 설정
# =========================
# PDF 앞부분 목차/표지 영역 때문에, section 감지는 이 페이지 이후부터 강하게 적용
MIN_PAGE_FOR_SECTION_DETECTION = 5

# 너무 짧은 섹션은 버림
MIN_SECTION_TEXT_LEN = 200

# 상품/보증/대출 관련 키워드
ANCHOR_KEYWORDS = [
    "보증",
    "대출",
    "상품안내",
    "약관",
    "주요내용",
    "보증한도",
    "보증이용절차",
    "전세",
    "임대",
    "분양",
    "주택",
]

# 미리 알고 있는 대표 상품/주제명(없어도 동작하지만 있으면 정확도 조금 개선)
CURATED_ANCHORS = [
    "전세보증금반환보증",
    "전세금안심대출보증",
    "임대보증금보증",
    "주택구입자금보증",
    "주택임차자금보증",
    "전세임대주택 전세보증금 반환보증",
    "임차료지급보증",
    "임대주택매입자금보증",
    "단독주택 품질보증",
    "단독주택 준공보증",
    "청년전용 월세대출보증",
    "주상복합주택분양보증",
    "보증이용절차",
    "보증한도",
    "보증별 주요내용",
    "보증별 약관",
]


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
    return name[:180] if name else "section"


def save_jsonl(rows: list[dict], path: Path):
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
def looks_like_bad_pdf_heading(line: str) -> bool:
    line = normalize_line(line)

    bad_patterns = [
        r"^\d+\s*[∙·]\s*주택도시보증공사$",
        r"^당당한 HUG, 청렴에서 시작합니다",
        r"^업 무 안 내$",
        r"^Ⅱ\.\s*보증별 상품안내$",
        r"^Ⅲ\.\s*보증이용절차$",
        r"^Ⅷ\.\s*보증별 주요내용$",
        r"^IX\.\s*보증별 약관$",
        r"^주택도시보증공사$",
    ]

    return any(re.search(p, line) for p in bad_patterns)


def is_real_anchor(anchor: str) -> bool:
    anchor = normalize_line(anchor)
    if looks_like_bad_pdf_heading(anchor):
        return False
    if len(anchor) < 4:
        return False
    if "주택도시보증공사" == anchor:
        return False
    return True



# =========================
# PDF 텍스트 추출
# =========================
def extract_pdf_pages(pdf_path: Path) -> list[dict]:
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF 파일이 없습니다: {pdf_path}")

    reader = PdfReader(str(pdf_path))
    pages = []

    for i, page in enumerate(reader.pages, start=1):
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        text = clean_text(text)

        pages.append({
            "page_no": i,
            "text": text,
        })

    return pages


# =========================
# 목차 후보 / 앵커 후보 추출
# =========================
def extract_toc_candidates(pages: list[dict], max_pages: int = 20) -> list[str]:
    """
    PDF 앞부분(표지/목차)에 있는 '상품명/주제명 + 페이지 번호' 형태를 최대한 잡아낸다.
    """
    candidates = OrderedDict()

    for page in pages[:max_pages]:
        lines = page["text"].split("\n")

        for line in lines:
            line = normalize_line(line)
            if not line:
                continue

            # 점선/페이지번호 제거 시도
            # 예: "전세보증금반환보증 ........ 53"
            line_wo_page = re.sub(r"\.{2,}\s*\d+\s*$", "", line)
            line_wo_page = re.sub(r"\s+\d+\s*$", "", line_wo_page)
            line_wo_page = normalize_line(line_wo_page)

            if len(line_wo_page) < 3 or len(line_wo_page) > 60:
                continue

            if any(k in line_wo_page for k in ANCHOR_KEYWORDS):
                candidates[line_wo_page] = True

    return list(candidates.keys())


def build_anchor_list(pages: list[dict]) -> list[str]:
    toc_candidates = extract_toc_candidates(pages, max_pages=20)

    anchors = OrderedDict()

    for a in CURATED_ANCHORS:
        if is_real_anchor(a):
            anchors[a] = True

    for a in toc_candidates:
        if is_real_anchor(a):
            anchors[a] = True

    return list(anchors.keys())


# =========================
# 페이지별 섹션 시작점 찾기
# =========================
def detect_anchor_in_page(page_text: str, anchors: list[str]) -> str | None:
    lines = [normalize_line(x) for x in page_text.split("\n")]
    lines = [x for x in lines if x][:50]

    # 페이지 상단의 머리글 제거
    lines = [x for x in lines if not looks_like_bad_pdf_heading(x)]

    header_zone = "\n".join(lines)
    anchors_sorted = sorted(anchors, key=len, reverse=True)

    for anchor in anchors_sorted:
        if anchor in header_zone and is_real_anchor(anchor):
            return anchor

    return None


def split_pages_into_sections(pages: list[dict], anchors: list[str]) -> list[dict]:
    """
    페이지 흐름을 따라가며 '새 anchor가 등장하는 페이지'를 섹션 시작점으로 본다.
    """
    sections = []
    current_section_title = None
    current_start_page = None
    current_text_parts = []

    for page in pages:
        page_no = page["page_no"]
        text = page["text"]

        detected_anchor = None
        if page_no >= MIN_PAGE_FOR_SECTION_DETECTION:
            detected_anchor = detect_anchor_in_page(text, anchors)

        # 새 섹션 시작
        if detected_anchor and detected_anchor != current_section_title:
            # 기존 섹션 마감
            if current_section_title and current_text_parts:
                section_text = clean_text("\n\n".join(current_text_parts))
                if len(section_text) >= MIN_SECTION_TEXT_LEN:
                    sections.append({
                        "section_title": current_section_title,
                        "page_start": current_start_page,
                        "page_end": page_no - 1,
                        "main_text": section_text,
                    })

            current_section_title = detected_anchor
            current_start_page = page_no
            current_text_parts = [text]
        else:
            # 아직 섹션 시작 전이면, 나중에 fallback용으로 무시
            if current_section_title:
                current_text_parts.append(text)

    # 마지막 섹션 처리
    if current_section_title and current_text_parts:
        section_text = clean_text("\n\n".join(current_text_parts))
        if len(section_text) >= MIN_SECTION_TEXT_LEN:
            sections.append({
                "section_title": current_section_title,
                "page_start": current_start_page,
                "page_end": pages[-1]["page_no"],
                "main_text": section_text,
            })

    return sections


# =========================
# 페이지 단위 fallback
# =========================
def build_page_level_records(pages: list[dict]) -> pd.DataFrame:
    rows = []

    for page in pages:
        rows.append({
            "source_org": "HUG",
            "source_type": "PDF_PAGE",
            "title": f"HUG 업무지침 2025.08 - 페이지 {page['page_no']}",
            "product_name": "",
            "url": "",
            "breadcrumb": "업무지침 PDF > 페이지단위",
            "page_start": page["page_no"],
            "page_end": page["page_no"],
            "main_text": page["text"],
            "collected_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    return pd.DataFrame(rows)


def build_section_records(sections: list[dict]) -> list[dict]:
    rows = []

    for sec in sections:
        title = sec["section_title"]
        rows.append({
            "source_org": "HUG",
            "source_type": "PDF_SECTION",
            "title": title,
            "product_name": title,
            "url": "",
            "breadcrumb": "업무지침 PDF > 섹션분리",
            "page_start": sec["page_start"],
            "page_end": sec["page_end"],
            "main_text": sec["main_text"],
            "collected_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    return rows

def samhammington():

    return None

# =========================
# 메인
# =========================
def main():
    print("[1] PDF 페이지 텍스트 추출")
    pages = extract_pdf_pages(PDF_PATH)
    print(f"  - 페이지 수: {len(pages)}")

    # 전체 텍스트 저장
    full_text = []
    for p in pages:
        full_text.append(f"[PAGE {p['page_no']}]\n{p['text']}")
    full_text = "\n\n".join(full_text).strip()
    OUTPUT_FULL_TXT.write_text(full_text, encoding="utf-8")
    print(f"  - 전체 TXT 저장: {OUTPUT_FULL_TXT}")

    # 페이지 단위 저장
    page_df = build_page_level_records(pages)
    page_df.to_csv(OUTPUT_PAGES_CSV, index=False, encoding="utf-8-sig")
    print(f"  - 페이지 CSV 저장: {OUTPUT_PAGES_CSV}")

    print("[2] 목차/상품명 후보 추출")
    anchors = build_anchor_list(pages)
    print(f"  - anchor 후보 수: {len(anchors)}")
    for a in anchors[:20]:
        print("   >", a)

    print("[3] 섹션 분리 시도")
    sections = split_pages_into_sections(pages, anchors)
    print(f"  - 섹션 수: {len(sections)}")

    section_rows = build_section_records(sections)

    # 섹션 분리가 거의 안 되면 최소한 전체 문서 1건이라도 남김
    if not section_rows:
        print("  - 섹션 분리가 충분히 되지 않아 fallback 문서 1건 생성")
        section_rows = [{
            "source_org": "HUG",
            "source_type": "PDF_FULL",
            "title": "HUG 업무지침 2025.08",
            "product_name": "HUG 업무지침 2025.08",
            "url": "",
            "breadcrumb": "업무지침 PDF > 전체문서",
            "page_start": 1,
            "page_end": len(pages),
            "main_text": full_text,
            "collected_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        }]

    section_df = pd.DataFrame(section_rows)

    # 품질 보조 컬럼
    section_df["text_len"] = section_df["main_text"].fillna("").str.len()
    section_df = section_df.sort_values(["page_start", "title"], ascending=[True, True]).reset_index(drop=True)

    section_df.to_csv(OUTPUT_SECTIONS_CSV, index=False, encoding="utf-8-sig")
    save_jsonl(section_rows, OUTPUT_SECTIONS_JSONL)

    print("완료")
    print(f"- 페이지 CSV  : {OUTPUT_PAGES_CSV}")
    print(f"- 섹션 CSV   : {OUTPUT_SECTIONS_CSV}")
    print(f"- 섹션 JSONL : {OUTPUT_SECTIONS_JSONL}")
    print(f"- 전체 TXT   : {OUTPUT_FULL_TXT}")


if __name__ == "__main__":
    main()