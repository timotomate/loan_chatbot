from pathlib import Path
from datetime import datetime
import re
import pandas as pd


# =========================================================
# 설정: 여기만 수정해서 사용
# =========================================================

# 엑셀 파일별 설정
# - institution: 파일명 prefix (예: HF, HUG, SGI)
# - excel_path: 원본 엑셀 경로
# - source_name: md의 출처 표기
# - default_product_group: 상품군 컬럼이 없을 때 기본값
# - default_category: 분류 컬럼이 없을 때 기본값
# - start_number: 파일 번호 시작값 (예: 821 -> HF_821.md부터 생성)
EXCEL_CONFIGS = [
    {
        "institution": "HF",
        "excel_path": r"C:\path\to\hf_faq.xlsx",
        "source_name": "하나은행 r가계여신_Q피드",
        "default_product_group": "HF 주신보",
        "default_category": "",
        "start_number": 821,
    },
    {
        "institution": "HUG",
        "excel_path": r"C:\path\to\hug_faq.xlsx",
        "source_name": "하나은행 r가계여신_Q피드",
        "default_product_group": "HUG",
        "default_category": "",
        "start_number": 242,
    },
    {
        "institution": "SGI",
        "excel_path": r"C:\path\to\sgi_faq.xlsx",
        "source_name": "하나은행 r가계여신_Q피드",
        "default_product_group": "SGI",
        "default_category": "",
        "start_number": 174,
    },
]

# 저장 폴더
OUTPUT_DIR = Path("./faq_output_md")

# 엑셀 컬럼 매핑
# 실제 엑셀 컬럼명에 맞게 오른쪽 값을 수정하면 된다.
# number 부분 수정
COLUMN_MAP = {
    "number": "번호",
    "title": "제목",
    "question": "질문",
    "answer": "답변",
    "product_group": "상품군",
    "category": "분류",
    "assetized_at": "자산화일시",
}


def clean_text(value):
    if pd.isna(value):
        return ""
    text = str(value).strip()
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def format_number(number_value):
    """
    번호를 3자리 zero-padding 문자열로 만든다.
    예: 7 -> 007
    """
    text = clean_text(number_value)
    if not text:
        return ""

    match = re.search(r"\d+", text)
    if not match:
        return text

    return f"{int(match.group()):03d}"


def format_assetized_at(value):
    if pd.isna(value) or str(value).strip() == "":
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    text = clean_text(value)
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
    ]:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    return text


def build_md_content(
    institution,
    number_padded,
    title,
    product_group,
    category,
    source_name,
    assetized_at,
    question,
    answer,
):
    return f"""# [{institution} FAQ #{int(number_padded)}] {title}

## [문서 정보]
* **상품군**: {product_group}
* **분류**: {category}
* **출처**: {source_name}
* **자산화일시**: {assetized_at}

---

### [현장 질문]
{question}


### [전문가 답변]
{answer}


---
"""


def get_value(row, logical_name, default_value=""):
    excel_col = COLUMN_MAP.get(logical_name, "")
    if excel_col and excel_col in row.index:
        value = row[excel_col]
        cleaned = clean_text(value)
        if cleaned != "":
            return cleaned
    return default_value


def export_one_excel(config):
    institution = config["institution"]
    excel_path = Path(config["excel_path"])
    source_name = config["source_name"]
    default_product_group = config.get("default_product_group", institution)
    default_category = config.get("default_category", "")
    start_number = int(config.get("start_number", 1))

    if not excel_path.exists():
        print(f"[SKIP] 파일 없음: {excel_path}")
        return 0

    df = pd.read_excel(excel_path)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    created_count = 0

    for row_idx, (_, row) in enumerate(df.iterrows(), start=0):
        # 번호 컬럼이 있으면 우선 사용하고, 없으면 start_number부터 순차 부여
        raw_number = get_value(row, "number")
        if raw_number:
            number_padded = format_number(raw_number)
        else:
            number_padded = f"{start_number + row_idx:03d}"
        title = get_value(row, "title")
        question = get_value(row, "question")
        answer = get_value(row, "answer")
        product_group = get_value(row, "product_group", default_product_group)
        category = get_value(row, "category", default_category)
        assetized_at = format_assetized_at(
            row[COLUMN_MAP["assetized_at"]]
            if COLUMN_MAP.get("assetized_at") in row.index
            else ""
        )

        if not number_padded:
            print("[SKIP] 번호 없음")
            continue

        if not title:
            title = f"{institution} FAQ {int(number_padded)}"

        if not question and not answer:
            print(f"[SKIP] 질문/답변 없음: {institution}_{number_padded}.md")
            continue

        md_content = build_md_content(
            institution=institution,
            number_padded=number_padded,
            title=title,
            product_group=product_group,
            category=category,
            source_name=source_name,
            assetized_at=assetized_at,
            question=question,
            answer=answer,
        )

        output_path = OUTPUT_DIR / f"{institution}_{number_padded}.md"
        output_path.write_text(md_content, encoding="utf-8")
        created_count += 1

    print(f"[OK] {institution}: {created_count}개 생성")
    return created_count


def export_all_excels():
    total = 0
    for config in EXCEL_CONFIGS:
        total += export_one_excel(config)

    print("")
    print(f"[DONE] 총 {total}개 md 파일 생성")
    print(f"[DONE] 저장 위치: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    export_all_excels()
