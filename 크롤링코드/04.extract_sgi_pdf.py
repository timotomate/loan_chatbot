import re
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
from pypdf import PdfReader


# =========================
# 경로 설정
# =========================
PDF_DIR = Path("sgi_pdfs")
RAW_DIR = Path("raw/sgi")
RAW_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DOC_CSV = RAW_DIR / "sgi_pdf_docs.csv"
OUTPUT_PAGE_CSV = RAW_DIR / "sgi_pdf_pages.csv"
OUTPUT_DOC_JSONL = RAW_DIR / "sgi_pdf_docs.jsonl"

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


def save_jsonl(rows: list[dict], path: Path):
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


# =========================
# 파일명 파싱
# =========================
DOC_TYPE_PATTERNS = {
    "보험약관": "보험약관",
    "사업방법서": "사업방법서",
    "상품요약서": "상품요약서",
    "상품설명서": "상품설명서",
}


def infer_doc_type(file_name: str) -> str:
    for key, value in DOC_TYPE_PATTERNS.items():
        if key in file_name:
            return value
    return "미분류문서"


def infer_base_product_name(file_name: str) -> str:
    """
    예:
    - 전세금보장신용보험(개인용) 보험약관_20250401.pdf
    - 전세금보장신용보험(개인용)_사업방법서_20260102.pdf
    - 전세금보장신용보험(개인용)_상품요약서_20250401.pdf
    """
    name = Path(file_name).stem

    # 날짜 제거
    name = re.sub(r"_?\d{8}$", "", name)

    # 문서유형 제거
    for key in DOC_TYPE_PATTERNS.keys():
        name = name.replace(key, "")

    # 언더스코어/공백 정리
    name = name.replace("__", "_")
    name = re.sub(r"[_]+", " ", name)
    name = re.sub(r"\s{2,}", " ", name)

    return name.strip(" _-")


def infer_doc_date(file_name: str) -> str:
    """
    파일명 끝의 8자리 날짜를 YYYY-MM-DD로 변환
    """
    name = Path(file_name).stem
    m = re.search(r"(\d{8})$", name)
    if not m:
        return ""

    ymd = m.group(1)
    try:
        dt = datetime.strptime(ymd, "%Y%m%d")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

# =========================
# PDF 텍스트 추출
# =========================
def extract_pdf_pages(pdf_path: Path) -> list[dict]:
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
# 제목 보정
# =========================
def infer_title_from_text(first_page_text: str, fallback_title: str) -> str:
    lines = [normalize_line(x) for x in first_page_text.split("\n")]
    lines = [x for x in lines if x]

    # 첫 페이지 초반에서 의미 있는 제목 후보 찾기
    for line in lines[:10]:
        if len(line) < 3:
            continue
        if any(k in line for k in ["보험약관", "사업방법서", "상품요약서", "상품설명서", "신용보험"]):
            return line

    return fallback_title


# =========================
# 메인
# =========================
def main():
    pdf_files = sorted(PDF_DIR.glob("*.pdf"))
    if not pdf_files:
        raise FileNotFoundError(f"PDF 파일이 없습니다: {PDF_DIR}")

    doc_rows = []
    page_rows = []

    print(f"[1] PDF 파일 수: {len(pdf_files)}")

    for pdf_path in pdf_files:
        file_name = pdf_path.name
        print(f" - 처리 중: {file_name}")

        doc_type = infer_doc_type(file_name)
        product_name = infer_base_product_name(file_name)
        doc_date = infer_doc_date(file_name)

        pages = extract_pdf_pages(pdf_path)
        full_text = "\n\n".join(
            [f"[PAGE {p['page_no']}]\n{p['text']}" for p in pages if p["text"]]
        ).strip()

        first_page_text = pages[0]["text"] if pages else ""
        title = infer_title_from_text(
            first_page_text=first_page_text,
            fallback_title=f"{product_name} {doc_type}".strip()
        )

        # 문서 단위 레코드
        doc_rows.append({
            "source_org": "SGI",
            "source_type": "PDF",
            "product_name": product_name,
            "document_type": doc_type,
            "title": title,
            "source_file": file_name,
            "doc_date": doc_date,
            "page_count": len(pages),
            "main_text": full_text,
            "collected_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

        # 페이지 단위 레코드
        for p in pages:
            page_rows.append({
                "source_org": "SGI",
                "source_type": "PDF_PAGE",
                "product_name": product_name,
                "document_type": doc_type,
                "title": title,
                "source_file": file_name,
                "doc_date": doc_date,
                "page_no": p["page_no"],
                "page_text": p["text"],
                "collected_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
            })

    doc_df = pd.DataFrame(doc_rows)
    page_df = pd.DataFrame(page_rows)

    doc_df.to_csv(OUTPUT_DOC_CSV, index=False, encoding="utf-8-sig")
    page_df.to_csv(OUTPUT_PAGE_CSV, index=False, encoding="utf-8-sig")
    save_jsonl(doc_rows, OUTPUT_DOC_JSONL)

    print("[완료]")
    print(f"- 문서 CSV : {OUTPUT_DOC_CSV}")
    print(f"- 페이지 CSV: {OUTPUT_PAGE_CSV}")
    print(f"- 문서 JSONL: {OUTPUT_DOC_JSONL}")


if __name__ == "__main__":
    main()