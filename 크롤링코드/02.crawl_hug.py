import re
import json
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse, urldefrag

import requests
import pandas as pd
from bs4 import BeautifulSoup


# =========================
# 경로 설정
# =========================
RAW_DIR = Path("raw/hug")
RAW_HTML_DIR = Path("raw_html/hug")

RAW_DIR.mkdir(parents=True, exist_ok=True)
RAW_HTML_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV = RAW_DIR / "hug_raw.csv"
OUTPUT_JSONL = RAW_DIR / "hug_raw.jsonl"
OUTPUT_LINKS_CSV = RAW_DIR / "hug_links.csv"


# =========================
# 시작 URL / 도메인 / 필터
# =========================
SEED_URLS = [
    "https://www.khug.or.kr/hug/web/ig/dr/igdr000007.jsp?tabMenu=Y",
    "https://www.khug.or.kr/hug/web/ig/dr/igdr000008.jsp?tabMenu=Y",
    "https://www.khug.or.kr/hug/web/ig/dr/igdr000009.jsp?tabFocus=Y",
    "https://www.khug.or.kr/hug/web/cg/sd/cgsd000001.jsp?tabMenu=Y",
    "https://www.khug.or.kr/hug/web/cg/sd/cgsd000002.jsp?tabMenu=Y",
    "https://www.khug.or.kr/hug/web/gb/eg/gbeg000007.jsp",
    "https://onestop.khug.or.kr/webView/webBiz/apply/goods001",
]

ALLOWED_DOMAINS = {
    "www.khug.or.kr",
    "onestop.khug.or.kr",
}

ALLOWED_PATH_PREFIXES = [
    "/hug/web/",
    "/webView/webBiz/",
]

EXCLUDE_KEYWORDS_IN_URL = [
    "login",
    "recruit",
    "notice",
    "news",
    "board",
    "download",
    "popup",
    "faq",
]

EXCLUDE_KEYWORDS_IN_TEXT = [
    "공지",
    "공지사항",
    "채용",
    "로그인",
    "회원가입",
    "보도자료",
    "팝업",
]

PRODUCT_HINT_KEYWORDS = [
    "보증",
    "보증금",
    "전세",
    "월세",
    "기금",
    "대출",
    "상품안내",
    "신청절차",
    "제출서류",
    "보증대상",
    "보증료",
    "신청기한",
    "가입",
    "임대보증금",
    "전세보증금반환보증",
    "전세금안심대출보증",
    "특례반환보증",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0 Safari/537.36"
    )
}

TIMEOUT = 20
REQUEST_SLEEP = 0.3
MAX_DISCOVER_PAGES = 300
MAX_CRAWL_PAGES = 500


# =========================
# 유틸
# =========================
def clean_text(text: str) -> str:
    if text is None:
        return ""
    text = str(text).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_url(url: str) -> str:
    url, _ = urldefrag(url)
    return url.strip()


def safe_filename(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    name = re.sub(r"\s+", "_", name).strip("_")
    return name[:180] if name else "page"


def is_allowed_domain(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and p.netloc in ALLOWED_DOMAINS
    except Exception:
        return False


def path_allowed(path: str) -> bool:
    return any(path.startswith(prefix) for prefix in ALLOWED_PATH_PREFIXES)


def looks_excluded_url(url: str) -> bool:
    low = url.lower()

    bad_patterns = [
        "/hug/web/en/",          # 영문 페이지
        "/hug/web/eh/",          # 경영공시/비상품 가능성 높은 영역
        "board",
        "bbs",
        "popup",
        "history",
        "mission",
        "vision",
        "organization",
        "chart",
        "ethical",
        "promotional",
        "video",
        "borrower",
        "lender",
        "pf",
        "statistics",
        "nhuf",
        "fulfilment",
        "issuance process",
    ]

    if any(k in low for k in EXCLUDE_KEYWORDS_IN_URL):
        return True

    if any(k in low for k in bad_patterns):
        return True

    return False


def looks_excluded_text(text: str) -> bool:
    text = clean_text(text)

    bad_texts = [
        "채용",
        "로그인",
        "회원가입",
        "CEO Message",
        "History",
        "Mission & Vision",
        "Organizational Chart",
        "Ethical Management",
        "Promotional Video",
        "Fulfilment Process",
        "Issuance Process",
        "Statistics",
        "NHUF",
        "Q ",
        "Q.",
    ]

    return any(k in text for k in bad_texts)


def looks_product_related(text: str) -> bool:
    text = clean_text(text)
    return any(k in text for k in PRODUCT_HINT_KEYWORDS)


def is_skip_link(href: str) -> bool:
    if not href:
        return True

    low = href.lower().strip()

    if low.startswith("javascript:"):
        return True
    if low.startswith("mailto:"):
        return True
    if low.startswith("tel:"):
        return True

    if any(low.endswith(ext) for ext in [
        ".pdf", ".hwp", ".hwpx", ".zip",
        ".doc", ".docx", ".xls", ".xlsx",
        ".ppt", ".pptx", ".jpg", ".jpeg",
        ".png", ".gif", ".svg", ".webp"
    ]):
        return True

    return False


# =========================
# 요청 / 파싱
# =========================
def fetch_html(url: str, session: requests.Session):
    try:
        resp = session.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()

        ctype = resp.headers.get("Content-Type", "")
        if "text/html" not in ctype:
            return None, None

        html_text = resp.text
        soup = BeautifulSoup(html_text, "lxml")
        return html_text, soup

    except Exception as e:
        print(f"[fetch] 실패: {url} / {e}")
        return None, None


def extract_links(soup: BeautifulSoup, base_url: str) -> list[dict]:
    found = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        anchor_text = clean_text(a.get_text(" ", strip=True))

        if is_skip_link(href):
            continue

        full = urljoin(base_url, href)
        full = normalize_url(full)

        if not is_allowed_domain(full):
            continue

        if full in seen:
            continue
        seen.add(full)

        found.append({
            "url": full,
            "anchor_text": anchor_text,
        })

    return found


def remove_noise(soup: BeautifulSoup) -> BeautifulSoup:
    for tag in soup.select(
        "script, style, noscript, iframe, form, button, "
        "header, footer, aside, .skip, .util, .search, .quick"
    ):
        tag.decompose()
    return soup


def pick_main_container(soup: BeautifulSoup):
    candidates = [
        "#contents",
        "#content",
        ".contents",
        ".content",
        ".sub-contents",
        ".contWrap",
        ".conArea",
        ".container",
        ".content-wrap",
        ".board_view",
        ".view_cont",
        "main",
        "body",
    ]
    for selector in candidates:
        node = soup.select_one(selector)
        if node and clean_text(node.get_text(" ", strip=True)):
            return node
    return soup.body or soup


def extract_title(soup: BeautifulSoup) -> str:
    for selector in ["h1", ".tit", ".title", ".subject", "title"]:
        node = soup.select_one(selector)
        if node:
            txt = clean_text(node.get_text(" ", strip=True))
            if txt:
                return txt
    return ""


def extract_breadcrumb(soup: BeautifulSoup) -> str:
    for selector in [".location", ".breadcrumb", ".path", ".navi", ".lnb"]:
        node = soup.select_one(selector)
        if node:
            txt = clean_text(node.get_text(" > ", strip=True))
            if txt:
                return txt
    return ""


def extract_headings(container) -> list[str]:
    vals = []
    for tag in container.find_all(["h1", "h2", "h3", "h4", "dt", "strong"]):
        txt = clean_text(tag.get_text(" ", strip=True))
        if txt and len(txt) <= 150:
            vals.append(txt)

    out = []
    seen = set()
    for x in vals:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def extract_lists(container) -> list[str]:
    items = []
    for li in container.find_all("li"):
        txt = clean_text(li.get_text(" ", strip=True))
        if txt:
            items.append(txt)
    return items


def extract_tables(container) -> list[dict]:
    tables = []
    for idx, table in enumerate(container.find_all("table"), start=1):
        rows = []
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            row = [clean_text(c.get_text(" ", strip=True)) for c in cells]
            if any(row):
                rows.append(row)
        if rows:
            tables.append({
                "table_index": idx,
                "rows": rows,
            })
    return tables


def extract_main_text(container) -> str:
    pieces = []
    for tag in container.find_all(["p", "li", "dt", "dd", "th", "td", "h1", "h2", "h3", "h4"]):
        txt = clean_text(tag.get_text(" ", strip=True))
        if txt:
            pieces.append(txt)

    compact = []
    prev = None
    for t in pieces:
        if t != prev:
            compact.append(t)
        prev = t

    return "\n".join(compact).strip()


def save_raw_html(url: str, html_text: str) -> str:
    netloc = urlparse(url).netloc
    name = safe_filename(netloc + "_" + url.replace("https://", "").replace("http://", "")) + ".html"
    path = RAW_HTML_DIR / name
    path.write_text(html_text, encoding="utf-8")
    return str(path)


def page_looks_like_product_page(url: str, title: str, breadcrumb: str, main_text: str, anchor_text: str = "") -> bool:
    parsed = urlparse(url)

    if not path_allowed(parsed.path):
        return False

    if looks_excluded_url(url):
        return False

    joined = " ".join([title, breadcrumb, anchor_text, main_text[:2000]])

    if looks_excluded_text(joined):
        return False

    # 반드시 상품 키워드가 하나 이상 있어야 함
    must_have = ["보증", "대출", "상품", "신청절차", "제출서류", "보증대상", "보증료", "전세", "임대"]
    if not any(k in joined for k in must_have):
        return False

    # 너무 짧은 페이지 제외
    if len(main_text) < 250:
        return False

    return True


# =========================
# 링크 탐색
# =========================
def discover_candidate_links(session: requests.Session) -> list[dict]:
    queue = list(SEED_URLS)
    visited = set()
    found_candidates = {}

    while queue and len(visited) < MAX_DISCOVER_PAGES:
        current = queue.pop(0)
        current = normalize_url(current)

        if current in visited:
            continue
        visited.add(current)

        print(f"[discover] 방문 중: {current} / visited={len(visited)} / found={len(found_candidates)}")

        html_text, soup = fetch_html(current, session)
        if not soup:
            continue

        links = extract_links(soup, current)

        for item in links:
            link = item["url"]
            anchor_text = item["anchor_text"]
            parsed = urlparse(link)

            if looks_excluded_url(link):
                continue

            if not path_allowed(parsed.path):
                continue

            if looks_excluded_text(anchor_text):
                continue

            is_candidate = (
                looks_product_related(anchor_text)
                or looks_product_related(link)
                or path_allowed(parsed.path)
            )

            if is_candidate and link not in found_candidates:
                found_candidates[link] = {
                    "url": link,
                    "anchor_text": anchor_text,
                    "discovered_from": current,
                }

            if (
                parsed.netloc in ALLOWED_DOMAINS
                and path_allowed(parsed.path)
                and link not in visited
                and link not in queue
            ):
                queue.append(link)

        time.sleep(REQUEST_SLEEP)

    return sorted(found_candidates.values(), key=lambda x: x["url"])


# =========================
# 본문 수집
# =========================
def crawl_page(session: requests.Session, url: str, discovered_anchor_text: str = "") -> dict | None:
    html_text, soup = fetch_html(url, session)
    if not soup:
        return None

    soup = remove_noise(soup)

    title = extract_title(soup)
    breadcrumb = extract_breadcrumb(soup)
    container = pick_main_container(soup)

    main_text = extract_main_text(container)
    headings = extract_headings(container)
    list_items = extract_lists(container)
    tables = extract_tables(container)

    if not title and not main_text:
        return None

    if not page_looks_like_product_page(url, title, breadcrumb, main_text, discovered_anchor_text):
        return None

    raw_html_path = save_raw_html(url, html_text)

    return {
        "source_org": "HUG",
        "title": title,
        "url": url,
        "breadcrumb": breadcrumb,
        "discovered_anchor_text": discovered_anchor_text,
        "main_text": main_text,
        "headings_json": json.dumps(headings, ensure_ascii=False),
        "lists_json": json.dumps(list_items, ensure_ascii=False),
        "tables_json": json.dumps(tables, ensure_ascii=False),
        "raw_html_path": raw_html_path,
        "collected_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# =========================
# 저장
# =========================
def save_jsonl(rows: list[dict], path: Path):
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


# =========================
# 메인
# =========================
def main():
    session = requests.Session()

    print("[1] HUG 후보 링크 탐색 시작")
    candidates = discover_candidate_links(session)
    print(f"  - 후보 링크 수: {len(candidates)}")

    links_df = pd.DataFrame(candidates)
    links_df.to_csv(OUTPUT_LINKS_CSV, index=False, encoding="utf-8-sig")

    print("[2] HUG 본문 수집 시작")
    docs = []

    for idx, item in enumerate(candidates[:MAX_CRAWL_PAGES], start=1):
        url = item["url"]
        anchor_text = item.get("anchor_text", "")
        print(f"  - ({idx}/{min(len(candidates), MAX_CRAWL_PAGES)}) {url}")

        doc = crawl_page(session, url, discovered_anchor_text=anchor_text)
        if doc:
            docs.append(doc)

        time.sleep(REQUEST_SLEEP)

    df = pd.DataFrame(docs)

    if not df.empty:
        df = df.drop_duplicates(subset=["url"]).copy()
        df["text_len"] = df["main_text"].fillna("").str.len()
        df = df.sort_values(["text_len", "url"], ascending=[False, True]).reset_index(drop=True)

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    save_jsonl(docs, OUTPUT_JSONL)

    print("완료")
    print(f"- 링크 목록 : {OUTPUT_LINKS_CSV}")
    print(f"- 원문 CSV  : {OUTPUT_CSV}")
    print(f"- 원문 JSONL: {OUTPUT_JSONL}")
    print(f"- 원본 HTML : {RAW_HTML_DIR}")


if __name__ == "__main__":
    main()