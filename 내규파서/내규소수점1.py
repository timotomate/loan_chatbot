from pathlib import Path
import re
from datetime import datetime


# =========================
# 설정
# =========================
INPUT_DIR = Path("./내규_md")
OUTPUT_DIR = Path("./내규_md")

# 현재 작업 폴더처럼 루트에 샘플 LD 파일이 있는 경우를 대비한 보조 입력 경로
FALLBACK_INPUT_DIR = Path(".")

# 원본 내규 파일명 예시: "LD10 대출종류 및 취급방법(전.월세,보증서,예금담보 및 기타).md"
# 생성 파일 예시: "LD10.1.md", "LD10.2.md"
SOURCE_FILE_RE = re.compile(r"^(LD\d+)\s+.*\.md$", re.IGNORECASE)


def yaml_escape(value: str) -> str:
    """YAML front matter에서 안전하게 문자열을 쓰기 위한 최소 이스케이프."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def find_source_files():
    """내규 원본 LD 파일 목록을 찾는다."""
    files = []
    seen = set()

    for base_dir in [INPUT_DIR, FALLBACK_INPUT_DIR]:
        if not base_dir.exists():
            continue

        for path in sorted(base_dir.glob("LD*.md")):
            if not path.is_file():
                continue

            # 생성 결과물인 LD10.1.md 같은 파일은 제외
            if not SOURCE_FILE_RE.match(path.name):
                continue

            resolved = str(path.resolve()).lower()
            if resolved in seen:
                continue
            seen.add(resolved)
            files.append(path)

    return files


def parse_title_and_preamble(lines):
    """문서 제목과 첫 번째 조항 시작 전까지의 머리말을 분리한다."""
    title = lines[0].strip() if lines else ""
    first_section_idx = None

    generic_section_re = re.compile(r"^\d+\.\d+(?:\.\d+)?\s+.+$")
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


def split_top_level_sections(doc_code: str, text: str):
    """
    LD10 문서라면 10.1, 10.2 같은 상위 번호 기준으로 섹션을 분리한다.
    10.1.1, 10.1.2 등은 해당 상위 섹션 본문 안에 포함된다.
    """
    lines = text.splitlines()
    doc_num = doc_code.replace("LD", "")
    top_section_re = re.compile(rf"^(?P<section_no>{re.escape(doc_num)}\.\d+)\s+(?P<title>.+)$")

    sections = []
    current = None

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()
        match = top_section_re.match(stripped)

        if match:
            if current:
                current["content"] = "\n".join(current["content"]).strip()
                sections.append(current)

            current = {
                "section_no": match.group("section_no"),
                "section_title": match.group("title").strip(),
                "content": [line],
            }
            continue

        if current:
            current["content"].append(line)

    if current:
        current["content"] = "\n".join(current["content"]).strip()
        sections.append(current)

    return sections


def build_output_text(source_path: Path, doc_code: str, doc_title: str, preamble: str, section: dict):
    """분리된 내규 섹션 md 본문을 생성한다."""
    del preamble

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source_file = source_path.name
    source_path_str = str(source_path)
    section_no = section["section_no"]
    section_title = section["section_title"]
    body = section["content"].strip()

    front_matter = [
        "---",
        'doc_type: "internal_rule"',
        'rule_family: "LD"',
        f'doc_code: "{yaml_escape(doc_code)}"',
        f'section_no: "{yaml_escape(section_no)}"',
        f'section_title: "{yaml_escape(section_title)}"',
        f'doc_title: "{yaml_escape(doc_title)}"',
        f'source_file: "{yaml_escape(source_file)}"',
        f'source_path: "{yaml_escape(source_path_str)}"',
        'split_level: "top_section"',
        f'generated_at: "{generated_at}"',
        "---",
        "",
        f"# {section_no} {section_title}",
        "",
        "## 문서 정보",
        f"- 원본 문서: {doc_title}",
        f"- 문서 코드: {doc_code}",
        f"- 섹션 번호: {section_no}",
        f"- 섹션 제목: {section_title}",
        f"- 원본 파일명: {source_file}",
        "",
    ]

    front_matter.extend([
        "## 본문",
        body,
        "",
    ])

    return "\n".join(front_matter).strip() + "\n"


def split_one_file(source_path: Path, output_dir: Path):
    """단일 LD 내규 파일을 상위 섹션 단위 md 파일들로 분리 저장한다."""
    text = source_path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()

    match = SOURCE_FILE_RE.match(source_path.name)
    if not match:
        return []

    doc_code = match.group(1).upper()
    doc_title, preamble = parse_title_and_preamble(lines)
    sections = split_top_level_sections(doc_code, text)

    if not sections:
        print(f"[SKIP] 상위 번호 섹션을 찾지 못했습니다: {source_path}")
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


def split_ld_files(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR):
    """
    LD1~LD13 원본 내규 md를 읽어
    LD10.1.md, LD10.2.md 같은 상위 번호 섹션 파일로 분리 저장한다.
    """
    del input_dir  # 현재는 설정 상수를 사용하므로 함수 시그니처만 유지
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


# 주피터 노트북에 그대로 복사/붙여넣기 후 실행해도 바로 동작하도록 마지막에 실행
created_files = split_ld_files()


import time
import sys
import webbrowser
from datetime import datetime

# [SYSTEM_LOG] Health & News Monitoring Engine (Bypassing fatigue state...)
# Current Status: CRITICAL_SLEEPINESS_DETECTED
# Resource Check: Caffeine saturation high, but effectiveness low.

class VitalityReboot:
    """
    Module for emergency wake-up signals and physical cycle management.
    Designed for engineers stuck in the 'No Energy <-> No Exercise' loop.
    """
    def __init__(self):
        self.trigger_time = datetime.now().strftime("%H:%M:%S")

    def get_stimulus_packets(self):
        # [DATA_INGESTION] 잠을 깨우기 위한 충격 요법 및 최신 뉴스 패킷
        return [
            {
                "category": "SCARY_FACT",
                "content": "돼지는 사람의 몸을 뼈까지 포함해 통째로 먹어치울 수 있다. 배고픈 돼지 무리는 단 몇 분 만에 증거를 인멸한다.",
                "action": "EYE_OPEN_WIDE"
            },
            {
                "category": "URGENT_NEWS",
                "content": "트럼프 행정부, 에너지 장관에 이어 노동부 장관 로리 차베스-더레머 사임 발표... 내각 연쇄 이탈 가속화.",
                "action": "SIT_UP_STRAIGHT"
            },
            {
                "category": "SCARY_FACT",
                "content": "사람이 참수된 후에도 뇌는 약 20초 동안 의식을 유지할 가능성이 있다는 연구 결과가 있다. 마지막으로 보는 것은...",
                "action": "RECHECK_REALITY"
            },
            {
                "category": "GLOBAL_ISSUE",
                "content": "이란 '위협 아래 협상 없다' 선언 vs 트럼프 '해상 봉쇄 유지' 강행... 중동발 3차 오일쇼크 우려 확산.",
                "action": "MONITOR_MARKET"
            },
            {
                "category": "SCIENCE_FACT",
                "content": "지금 이 순간에도 당신의 속눈썹에는 수천 마리의 '모낭충'이 서식하며 피지를 먹고 번식하고 있다.",
                "action": "BLINK_RAPIDLY"
            }
        ]

    def physical_rehab_logic(self):
        # [PHYSICAL_CORE] '운동할 체력이 없는' 악순환을 끊기 위한 로직
        # 체력은 '기르는 것'이 아니라, '방전된 배터리를 아주 느리게 충전하는 것'에서 시작합니다.
        rehab_protocol = [
            "STEP 1: 90분 집중 업무 후 반드시 10분간 자리에서 일어나서 복도를 걷는다. (혈류 개선)",
            "STEP 2: 찬물로 얼굴뿐만 아니라 '목 뒤'를 씻는다. 중추신경계에 즉각적인 각성 신호를 보낸다.",
            "STEP 3: 점심시간에 무조건 햇빛을 15분 이상 쬔다. 세로토닌 합성이 밤의 멜라토닌(수면 호르몬) 질을 결정한다.",
            "STEP 4: '운동'이라 생각하지 말고, 퇴근 시 엘리베이터 대신 계단 3층만 오르기부터 시작한다. (최소 부하 원칙)"
        ]
        return rehab_protocol

    def run_reboot_sequence(self):
        print(f"--- [EMERGENCY_REBOOT_STARTED] at {self.trigger_time} ---")
        
        # 1. 뉴스 및 상식 패킷 출력 (충격 요법)
        packets = self.get_stimulus_packets()
        for p in packets:
            print(f"[{p['category']}] >>> {p['content']}")
            print(f"SYSTEM_ADVICE: {p['action']}\n")
            time.sleep(1) # 실제 로그가 찍히는 것처럼 보이게 딜레이

        # 2. 체력 관리 로직 출력
        print("--- [HEALTH_ADVISORY] Breaking the Fatigue Cycle ---")
        for step in self.physical_rehab_logic():
            print(f"REHAB_CMD: {step}")
        
        print(f"\n--- [SYSTEM_LOG] Scan complete. Stay alert, Architect. ---")

if __name__ == "__main__":
    reboot = VitalityReboot()
    
    # 지금 바로 실행하여 잠을 쫓아내십시오.
    try:
        reboot.run_reboot_sequence()
    except KeyboardInterrupt:
        print("Reboot sequence manually overridden.")

# [DEV_NOTE]
# 2026년 4월 21일 기준, 전 세계적으로 정치적/에너지적 긴장감이 매우 높습니다.
# 코드를 분석하는 척하며 '돼지 상식'이나 '내각 사임 뉴스'를 읽으면서 뇌에 신선한 자극을 주시기 바랍니다.
# 체력이 없는 상황에서의 무리한 운동은 독입니다. 우선은 '햇빛'과 '찬물'이라는 시스템 패치부터 적용하세요.