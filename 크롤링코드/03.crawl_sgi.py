import re
import json
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup


# =========================
# 경로 설정
# =========================
RAW_DIR = Path("raw/sgi")
RAW_HTML_DIR = Path("raw_html/sgi")

RAW_DIR.mkdir(parents=True, exist_ok=True)
RAW_HTML_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV = RAW_DIR / "sgi_raw.csv"
OUTPUT_JSONL = RAW_DIR / "sgi_raw.jsonl"
OUTPUT_LINKS_CSV = RAW_DIR / "sgi_links.csv"


# =========================
# 직접 수집할 seed URL
# www + m 혼합
# =========================
SEED_URLS = [
    # www.sgic.co.kr
    "https://www.sgic.co.kr/biz/ccp/index.html?p=CCPPRD030101F01",  # 전세금보장신용보험
    "https://www.sgic.co.kr/biz/ccp/index.html?p=CCPUTL030001F01",  # 상품공시
    "https://www.sgic.co.kr/biz/ccp/index.html?p=CCPUTL030002F01",  # 판매상품

    # m.sgic.co.kr - 개인/주거/생활 상품 중심
    "https://m.sgic.co.kr/biz/cmw/index.html?p=CMWPRD030301F01",    # 전세금반환보증보험
    "https://m.sgic.co.kr/biz/cmw/index.html?p=CMWPRD030501F01",    # 임대주택보증보험
    "https://m.sgic.co.kr/biz/cmw/index.html?p=CMWPRD030901F01",    # 생활안정자금보증보험
    "https://m.sgic.co.kr/biz/cmw/index.html?p=CMWPRD020401F01",    # 지급보증보험
]

ALLOWED_DOMAINS = {
    "www.sgic.co.kr",
    "m.sgic.co.kr",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0 Safari/537.36"
    )
}

TIMEOUT = 20
REQUEST_SLEEP = 0.25


# =========================
# 필터
# =========================
EXCLUDE_KEYWORDS_IN_URL = [
    "login", "logout", "recruit", "notice", "news", "board",
    "download", "faq", "qna", "popup", "cert", "auth",
    "install", "security", "ccputl010014"
]

EXCLUDE_KEYWORDS_IN_TEXT = [
    "로그인", "로그아웃", "공지사항", "채용", "보도자료",
    "FAQ", "Q&A", "보안프로그램", "인증서", "설치"
]

PRODUCT_HINT_KEYWORDS = [
    "전세금보장신용보험",
    "전세금반환보증보험",
    "임대주택보증보험",
    "생활안정자금보증보험",
    "지급보증보험",
    "보증보험",
    "신용보험",
    "상품공시",
    "판매상품",
    "가입대상",
    "가입방법",
    "필요서류",
    "보험료",
    "보장내용",
    "전세",
    "임대",
    "생활",
    "개인금융보증",
]


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
    url = url.strip()
    url = re.sub(r"^http://", "https://", url, flags=re.I)
    return url

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

def looks_excluded_url(url: str) -> bool:
    low = url.lower()
    return any(k in low for k in EXCLUDE_KEYWORDS_IN_URL)

def looks_excluded_text(text: str) -> bool:
    text = clean_text(text)
    return any(k in text for k in EXCLUDE_KEYWORDS_IN_TEXT)

def looks_product_related(text: str) -> bool:
    text = clean_text(text)
    return any(k in text for k in PRODUCT_HINT_KEYWORDS)


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


def remove_noise(soup: BeautifulSoup) -> BeautifulSoup:
    for tag in soup.select(
        "script, style, noscript, iframe, form, button, "
        "header, footer, aside, .skip, .util, .search, .quick"
    ):
        tag.decompose()
    return soup


def pick_main_container(soup: BeautifulSoup):
    candidates = [
        "#contents", "#content", ".contents", ".content", ".sub-contents",
        ".contWrap", ".conArea", ".container", ".content-wrap",
        ".board_view", ".view_cont", "main", "body",
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
    name = safe_filename(url.replace("https://", "").replace("http://", "")) + ".html"
    path = RAW_HTML_DIR / name
    path.write_text(html_text, encoding="utf-8")
    return str(path)


def page_looks_like_product_page(url: str, title: str, breadcrumb: str, main_text: str) -> bool:
    if looks_excluded_url(url):
        return False

    joined = " ".join([title, breadcrumb, main_text[:2000]])

    if looks_excluded_text(joined):
        return False

    if looks_product_related(joined):
        return True

    if len(main_text) >= 250 and (
        "CCPPRD" in url or
        "CCPUTL03000" in url or
        "CMWPRD03" in url or
        "CMWPRD02" in url
    ):
        return True

    return False


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

    print("[1] SGI seed URL 직접 수집 시작")
    candidates = []
    seen = set()

    for url in SEED_URLS:
        url = normalize_url(url)
        if url in seen:
            continue
        seen.add(url)
        candidates.append({
            "url": url,
            "anchor_text": "",
            "discovered_from": "seed",
        })

    links_df = pd.DataFrame(candidates)
    links_df.to_csv(OUTPUT_LINKS_CSV, index=False, encoding="utf-8-sig")
    print(f"  - seed URL 수: {len(candidates)}")

    print("[2] SGI 본문 수집 시작")
    docs = []

    for idx, item in enumerate(candidates, start=1):
        url = item["url"]
        print(f"  - ({idx}/{len(candidates)}) {url}")

        html_text, soup = fetch_html(url, session)
        if not soup:
            time.sleep(REQUEST_SLEEP)
            continue

        soup = remove_noise(soup)

        title = extract_title(soup)
        breadcrumb = extract_breadcrumb(soup)
        container = pick_main_container(soup)

        main_text = extract_main_text(container)
        headings = extract_headings(container)
        list_items = extract_lists(container)
        tables = extract_tables(container)

        if not title and not main_text:
            time.sleep(REQUEST_SLEEP)
            continue

        if not page_looks_like_product_page(url, title, breadcrumb, main_text):
            time.sleep(REQUEST_SLEEP)
            continue

        raw_html_path = save_raw_html(url, html_text)

        docs.append({
            "source_org": "SGI",
            "title": title,
            "url": url,
            "breadcrumb": breadcrumb,
            "discovered_anchor_text": "",
            "main_text": main_text,
            "headings_json": json.dumps(headings, ensure_ascii=False),
            "lists_json": json.dumps(list_items, ensure_ascii=False),
            "tables_json": json.dumps(tables, ensure_ascii=False),
            "raw_html_path": raw_html_path,
            "collected_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

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