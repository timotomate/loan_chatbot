from pathlib import Path
import re
from datetime import datetime


INPUT_DIR = Path("./내규_md")
OUTPUT_DIR = Path("./내규_md")
FALLBACK_INPUT_DIR = Path(".")

# Example source filename: "LD10 대출종류 및 취급방법(...).md"
SOURCE_FILE_RE = re.compile(r"^(LD\d+)\s+.*\.md$", re.IGNORECASE)


def yaml_escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def normalize_heading_prefix(text: str) -> str:
    """Strip markdown heading markers before section-number matching."""
    return re.sub(r"^\s*#+\s*", "", text).strip()
    

def find_source_files():
    files = []
    seen = set()

    for base_dir in [INPUT_DIR, FALLBACK_INPUT_DIR]:
        if not base_dir.exists():
            continue

        for path in sorted(base_dir.glob("LD*.md")):
            if not path.is_file():
                continue
            if not SOURCE_FILE_RE.match(path.name):
                continue

            resolved = str(path.resolve()).lower()
            if resolved in seen:
                continue

            seen.add(resolved)
            files.append(path)

    return files


def parse_title_and_preamble(lines):
    title = lines[0].strip() if lines else ""
    first_section_idx = None
    generic_section_re = re.compile(r"^(?:##\s*)?\d+\.\d+(?:\.\d+)?\s+.+$")

    for idx, line in enumerate(lines):
        if generic_section_re.match(line.strip()):
            first_section_idx = idx
            break

    if first_section_idx is None:
        preamble_lines = lines[:]
    else:
        preamble_lines = lines[:first_section_idx]

    preamble = "\n".join(preamble_lines).strip()
    return title, preamble


def split_level2_sections(doc_code: str, text: str):
    """
    For an LD document, split by level-2 sections such as 10.1.1, 10.1.2.
    Keep the top-level section info such as 10.1 as metadata.
    """
    lines = text.splitlines()
    doc_num = doc_code.replace("LD", "")
    top_section_re = re.compile(rf"^(?P<section_no>{re.escape(doc_num)}\.\d+)\s+(?P<title>.+)$")
    level2_re = re.compile(rf"^(?P<section_no>{re.escape(doc_num)}\.\d+\.\d+)\s+(?P<title>.+)$")

    sections = []
    current_top = None
    current_level2 = None

    def flush_current():
        nonlocal current_level2
        if not current_level2:
            return
        current_level2["content"] = "\n".join(current_level2["content"]).strip()
        sections.append(current_level2)
        current_level2 = None

    for raw_line in lines:
        line = raw_line.rstrip()
        normalized = normalize_heading_prefix(line)

        top_match = top_section_re.match(normalized)
        level2_match = level2_re.match(normalized)

        if top_match and not level2_match:
            current_top = {
                "section_no": top_match.group("section_no"),
                "section_title": top_match.group("title").strip(),
            }
            if current_level2:
                current_level2["content"].append(line)
            continue

        if level2_match:
            flush_current()

            if current_top and level2_match.group("section_no").startswith(current_top["section_no"] + "."):
                parent_no = current_top["section_no"]
                parent_title = current_top["section_title"]
            else:
                parent_no = ".".join(level2_match.group("section_no").split(".")[:2])
                parent_title = None

            current_level2 = {
                "section_no": level2_match.group("section_no"),
                "section_title": level2_match.group("title").strip(),
                "parent_section_no": parent_no,
                "parent_section_title": parent_title,
                "content": [line],
            }
            continue

        if current_level2:
            current_level2["content"].append(line)

    flush_current()
    return sections


def build_output_text(source_path: Path, doc_code: str, doc_title: str, preamble: str, section: dict):
    del preamble

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source_file = source_path.name
    source_path_str = str(source_path)
    section_no = section["section_no"]
    section_title = section["section_title"]
    parent_section_no = section.get("parent_section_no") or ""
    parent_section_title = section.get("parent_section_title") or ""
    body = section["content"].strip()

    output_lines = [
        "---",
        'doc_type: "internal_rule"',
        'rule_family: "LD"',
        f'doc_code: "{yaml_escape(doc_code)}"',
        f'parent_section_no: "{yaml_escape(parent_section_no)}"',
        f'parent_section_title: "{yaml_escape(parent_section_title)}"',
        f'section_no: "{yaml_escape(section_no)}"',
        f'section_title: "{yaml_escape(section_title)}"',
        f'doc_title: "{yaml_escape(doc_title)}"',
        f'source_file: "{yaml_escape(source_file)}"',
        f'source_path: "{yaml_escape(source_path_str)}"',
        'split_level: "level2_section"',
        f'generated_at: "{generated_at}"',
        "---",
        "",
        f"# {section_no} {section_title}",
        "",
        "## 문서 정보",
        f"- 원본 문서: {doc_title}",
        f"- 문서 코드: {doc_code}",
        f"- 상위 섹션 번호: {parent_section_no}",
        f"- 상위 섹션 제목: {parent_section_title}",
        f"- 섹션 번호: {section_no}",
        f"- 섹션 제목: {section_title}",
        f"- 원본 파일명: {source_file}",
        "",
        "## 본문",
        body,
        "",
    ]

    return "\n".join(output_lines).strip() + "\n"


def split_one_file(source_path: Path, output_dir: Path):
    text = source_path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()

    match = SOURCE_FILE_RE.match(source_path.name)
    if not match:
        return []

    doc_code = match.group(1).upper()
    doc_title, preamble = parse_title_and_preamble(lines)
    sections = split_level2_sections(doc_code, text)

    if not sections:
        print(f"[SKIP] 세부 섹션을 찾지 못했습니다: {source_path}")
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    created_files = []

    for section in sections:
        output_name = f"{section['section_no']}.md"
        output_path = output_dir / output_name
        output_text = build_output_text(
            source_path=source_path,
            doc_code=doc_code,
            doc_title=doc_title,
            preamble=preamble,
            section=section,
        )
        output_path.write_text(output_text, encoding="utf-8")
        created_files.append(output_path)

    print(f"[OK] {source_path.name} -> {len(created_files)}개 생성")
    return created_files


def split_ld_files_level2(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR):
    del input_dir
    del output_dir

    source_files = find_source_files()
    if not source_files:
        print("[INFO] 처리할 LD 원본 파일을 찾지 못했습니다.")
        print("       기대 파일명 예시: LD10 대출종류 및 취급방법(...).md")
        print("       검색 위치: ./내규_md, 현재 작업 폴더")
        return []

    all_created = []
    for source_path in source_files:
        created = split_one_file(source_path=source_path, output_dir=OUTPUT_DIR)
        all_created.extend(created)

    print("")
    print(f"[DONE] 총 {len(source_files)}개 원본 파일 처리")
    print(f"[DONE] 총 {len(all_created)}개 분리 파일 생성")
    print(f"[DONE] 저장 위치: {OUTPUT_DIR.resolve()}")
    return all_created


created_files = split_ld_files_level2()
