from pathlib import Path
from datetime import datetime
import re
import pandas as pd


# =========================================================
# HF 전용 설정
# =========================================================
EXCEL_PATH = Path(r"./테스트용/HF큐피드데이터(2025.12)_.xlsx")
OUTPUT_DIR = Path("./테스트용")

INSTITUTION = "HF"
START_NUMBER = 821
SOURCE_NAME = "하나은행 r가계여신_Q피드"
DEFAULT_PRODUCT_GROUP = "HF 주신보"
DEFAULT_CATEGORY = ""


def clean_text(value):
    if pd.isna(value):
        return ""
    text = str(value).strip()
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def build_md_content(number_padded, title, question, answer):
    assetized_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return f"""# [{INSTITUTION} FAQ #{int(number_padded)}] {title}

## [문서 정보]
* **상품군**: {DEFAULT_PRODUCT_GROUP}
* **분류**: {DEFAULT_CATEGORY}
* **출처**: {SOURCE_NAME}
* **자산화일시**: {assetized_at}

---

### [현장 질문]
{question}


### [전문가 답변]
{answer}


---
"""


def export_hf_excel_to_md():
    if not EXCEL_PATH.exists():
        print(f"[SKIP] 파일 없음: {EXCEL_PATH}")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 4행부터 시작, D열=질문, E열=답변
    # pandas 기준으로 skiprows=3 이면 엑셀 1~3행을 건너뛴다.
    df = pd.read_excel(
        EXCEL_PATH,
        header=None,
        skiprows=3,
        usecols="D:E",
        names=["question", "answer"],
    )

    created_count = 0

    for row_idx, row in df.iterrows():
        question = clean_text(row["question"])
        answer = clean_text(row["answer"])

        if not question and not answer:
            continue

        number_padded = f"{START_NUMBER + created_count:03d}"
        title = f"{INSTITUTION} FAQ {int(number_padded)}"
        md_content = build_md_content(
            number_padded=number_padded,
            title=title,
            question=question,
            answer=answer,
        )

        output_path = OUTPUT_DIR / f"{INSTITUTION}_{number_padded}.md"
        output_path.write_text(md_content, encoding="utf-8")
        created_count += 1

    print(f"[OK] {INSTITUTION}: {created_count}개 생성")
    print(f"[DONE] 저장 위치: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    export_hf_excel_to_md()

