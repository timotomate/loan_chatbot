import argparse
import json
import re
from pathlib import Path


SOURCE_FILE = "LD10 대출종류 및 취급방법(전.월세,보증서,예금담보 및 기타).md"


def load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def trim_front_matter(text: str) -> str:
    """Drop revision-history text before the first numbered rule section."""
    match = re.search(r"^\d+\.\d+(?:\.\d+)?\s+.+$", text, re.MULTILINE)
    return text[match.start():].strip() if match else text.strip()


def split_large_text(text: str, max_chars: int = 1200, overlap: int = 150) -> list[str]:
    """Fallback splitter for unusually long sections."""
    if len(text) <= max_chars:
        return [text]

    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks: list[str] = []
    current = ""

    for paragraph in paragraphs:
        candidate = f"{current}\n\n{paragraph}".strip() if current else paragraph
        if len(candidate) <= max_chars:
            current = candidate
            continue

        if current:
            chunks.append(current)

        if len(paragraph) <= max_chars:
            current = paragraph
            continue

        step = max_chars - overlap
        for i in range(0, len(paragraph), step):
            piece = paragraph[i:i + max_chars].strip()
            if piece:
                chunks.append(piece)
        current = ""

    if current:
        chunks.append(current)

    return chunks


def parse_ld10(text: str) -> list[dict]:
    """
    Parse LD10 style numbered rules into retrieval-friendly sections.

    Major headings:
      10.1 우량주택전세론
      10.1.8 대출한도

    Sub-items:
      1. 상품개요
      2. 대출신청시기
    """
    text = trim_front_matter(text)
    lines = text.splitlines()

    title = SOURCE_FILE.replace(".md", "")
    current_major: dict | None = None
    current_sub: dict | None = None
    parsed_sections: list[dict] = []

    major_pattern = re.compile(r"^(?P<num>\d+\.\d+(?:\.\d+)?)\s+(?P<title>.+)$")
    sub_pattern = re.compile(r"^(?P<num>\d+)\.\s+(?P<title>.+)$")

    def flush_sub() -> None:
        nonlocal current_sub
        if not current_sub:
            return
        current_sub["body"] = "\n".join(current_sub["body"]).strip()
        parsed_sections.append(current_sub)
        current_sub = None

    def flush_major_without_sub() -> None:
        nonlocal current_major
        if not current_major:
            return
        body = "\n".join(current_major["body"]).strip()
        if body:
            parsed_sections.append(
                {
                    "doc_title": title,
                    "major_no": current_major["major_no"],
                    "major_title": current_major["major_title"],
                    "sub_no": None,
                    "sub_title": None,
                    "body": body,
                }
            )
        current_major = None

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            if current_sub:
                current_sub["body"].append("")
            elif current_major:
                current_major["body"].append("")
            continue

        major_match = major_pattern.match(stripped)
        if major_match:
            flush_sub()
            flush_major_without_sub()
            current_major = {
                "major_no": major_match.group("num"),
                "major_title": major_match.group("title").strip(),
                "body": [],
            }
            continue

        sub_match = sub_pattern.match(stripped)
        if sub_match and current_major:
            flush_sub()
            current_sub = {
                "doc_title": title,
                "major_no": current_major["major_no"],
                "major_title": current_major["major_title"],
                "sub_no": sub_match.group("num"),
                "sub_title": sub_match.group("title").strip(),
                "body": [],
            }
            continue

        if current_sub:
            current_sub["body"].append(line)
        elif current_major:
            current_major["body"].append(line)

    flush_sub()
    flush_major_without_sub()

    return parsed_sections


def chunk_parsed_sections(parsed_sections: list[dict], max_chars: int = 1200) -> list[dict]:
    chunks: list[dict] = []

    for section in parsed_sections:
        header_lines = [
            f"[문서] {section['doc_title']}",
            f"[대분류] {section['major_no']} {section['major_title']}",
        ]
        if section["sub_no"] and section["sub_title"]:
            header_lines.append(f"[세부항목] {section['sub_no']}. {section['sub_title']}")

        header = "\n".join(header_lines)
        body_chunks = split_large_text(section["body"], max_chars=max_chars)

        for idx, body_chunk in enumerate(body_chunks, start=1):
            chunk_text = f"{header}\n\n{body_chunk}".strip()
            chunks.append(
                {
                    "chunk_id": len(chunks) + 1,
                    "major_no": section["major_no"],
                    "major_title": section["major_title"],
                    "sub_no": section["sub_no"],
                    "sub_title": section["sub_title"],
                    "part_no": idx,
                    "text": chunk_text,
                    "text_len": len(chunk_text),
                }
            )

    return chunks


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse and chunk the LD10 markdown file.")
    parser.add_argument("--max-chars", type=int, default=1200, help="Max chars per chunk body.")
    parser.add_argument("--preview", type=int, default=5, help="Number of chunks to preview.")
    parser.add_argument(
        "--save-json",
        action="store_true",
        help="Save parsed chunks as ld10_chunks.json for inspection.",
    )
    args = parser.parse_args()

    path = Path(SOURCE_FILE)
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")

    text = load_text(path)
    parsed_sections = parse_ld10(text)
    chunks = chunk_parsed_sections(parsed_sections, max_chars=args.max_chars)

    print(f"Parsed sections: {len(parsed_sections)}")
    print(f"Generated chunks: {len(chunks)}")
    print("")

    for chunk in chunks[: args.preview]:
        print("=" * 80)
        print(
            f"chunk_id={chunk['chunk_id']} | "
            f"major={chunk['major_no']} | "
            f"sub={chunk['sub_no'] or '-'} | "
            f"part={chunk['part_no']} | "
            f"len={chunk['text_len']}"
        )
        print(chunk["text"][:800])
        print("")

    if args.save_json:
        output_path = Path("ld10_chunks.json")
        output_path.write_text(
            json.dumps(chunks, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"Saved chunk data to: {output_path}")


if __name__ == "__main__":
    main()
