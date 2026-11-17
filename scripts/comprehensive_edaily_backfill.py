import os
import sys
import re
import html
import json
import time
import urllib.parse
import urllib3
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

urllib3.disable_warnings()

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from data_pipeline import (
    MediaDatabase,
    PersonnelTrackerEngine,
    PersonnelRegexParser,
    is_media_organization,
    is_excluded_department,
    is_personnel_article_title,
    normalize_media_name
)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
}

all_articles: Dict[str, Dict[str, Any]] = {}

def add_art(url: str, title: str, src: str, date_hint: str = ""):
    if not url or not title:
        return
    u = url.strip()
    if u in all_articles:
        return
    all_articles[u] = {
        "url": u,
        "title": title.strip(),
        "source": src,
        "date_hint": date_hint.strip()
    }

print("=== [1단계] 이데일리 후보 인사 기사 수집 시작 ===")

# 1. journalist.or.kr (한국기자협회보)
print("1. 한국기자협회(journalist.or.kr) 수집...")
for jq in ["이데일리", "이데일리M"]:
    for page in range(1, 26):
        url = f"https://www.journalist.or.kr/news/search_result.html?search={urllib.parse.quote(jq)}&sort=1&page={page}"
        try:
            r = requests.get(url, headers=headers, timeout=5, verify=False)
            if r.status_code != 200:
                break
            soup = BeautifulSoup(r.content, 'html.parser')
            anchors = soup.find_all('a')
            found = 0
            for a in anchors:
                h = a.get('href', '')
                t = a.get_text(strip=True)
                if 'article.html?no=' in h and t:
                    if any(k in t for k in ['[인사]', '<인사>', '인사', '선임', '승진', '보임', '전보']):
                        full_u = f"https://www.journalist.or.kr/news/{h}" if not h.startswith('http') else h
                        add_art(full_u, t, "journalist")
                        found += 1
            if found == 0 and page > 3:
                break
        except Exception:
            break
print(f"   -> 기자협회보 완료 (누적: {len(all_articles)}건)")

# 2. Daum News 연도별(2005~2026) 수집
print("2. Daum News 연도별(2005~2026) 수집...")
daum_queries = [
    '[인사] 이데일리',
    '[인사]이데일리',
    '이데일리 [인사]',
    '이데일리 인사',
    '이데일리 "편집국" [인사]',
    '이데일리 "건설부동산"',
    '이데일리 "디지털미디어"',
    '이데일리 "마케팅"'
]

for yr in range(2005, 2027):
    for q in daum_queries:
        for p in range(1, 3):
            url = f"https://search.daum.net/search?w=news&nil_search=btn&DA=STC&enc=utf8&cluster=y&cluster_page=1&q={urllib.parse.quote(q)}&sd={yr}0101000000&ed={yr}1231235959&period=u&p={p}"
            try:
                r = requests.get(url, headers=headers, timeout=4, verify=False)
                if r.status_code != 200:
                    break
                soup = BeautifulSoup(r.content, 'html.parser')
                items = soup.select('a.tit_main, a.f_link_b, a[data-tiara-layer="article_title"]')
                if not items:
                    break
                for a in items:
                    t = a.get_text(strip=True)
                    h = a.get('href', '')
                    if any(k in t for k in ['[인사]', '<인사>', '인사', '선임', '승진', '보임', '전보']) and '이데일리' in t:
                        add_art(h, t, "daum", f"{yr}-01-01")
            except Exception:
                pass
print(f"   -> Daum 완료 (누적: {len(all_articles)}건)")

# 3. Naver News 연도별 구간 수집
print("3. Naver News 연도별 구간 수집...")
naver_queries = [
    '"[인사] 이데일리"',
    '"[인사]이데일리"',
    '"이데일리" [인사] 연합뉴스',
    '"이데일리" [인사] 뉴스1',
    '"이데일리" [인사] 뉴시스',
    '"이데일리" [인사] 한국경제',
    '"이데일리" [인사] 매일경제',
    '"이데일리" [인사] 한겨레',
    '"◆이데일리"',
    '"◇이데일리"',
    '"[이데일리]"'
]

intervals = [
    (2005, 2007), (2008, 2010), (2011, 2013),
    (2014, 2016), (2017, 2019), (2020, 2022),
    (2023, 2026)
]

for sy, ey in intervals:
    for nq in naver_queries:
        for start_idx in [1, 11]:
            url = f"https://search.naver.com/search.naver?where=news&query={urllib.parse.quote(nq)}&sort=0&pd=3&ds={sy}.01.01&de={ey}.12.31&nso=so:r,p:from{sy}0101to{ey}1231,a:all&start={start_idx}"
            try:
                r = requests.get(url, headers=headers, timeout=4, verify=False)
                if r.status_code != 200:
                    continue
                soup = BeautifulSoup(r.content, 'html.parser')
                for a in soup.select('a.news_tit, a.W5fYO2_0xMDeLrlx, a[href*="news.naver.com"]'):
                    h = a.get('href', '')
                    t = a.get_text(strip=True).replace('새 창 열림', '').replace('새창열림', '').strip()
                    if not h or not t:
                        continue
                    if any(w in t for w in ['[인사]', '<인사>', '인사', '선임', '승진', '보임', '전보']) and len(t) < 80:
                        add_art(h, t, "naver", f"{sy}-01-01")
            except Exception:
                pass

print(f"\n=======================================================")
print(f"총 수집된 이데일리 인사 후보 기사: {len(all_articles)}건")
print(f"=======================================================")

# [2단계] 기사 본문 추출 및 정밀 날짜 파싱
print("\n=== [2단계] 기사 본문 수집 및 정밀 일자/발령 파싱 ===")

def extract_exact_date(url: str, soup: BeautifulSoup, date_hint: str) -> str:
    # 1. URL 패턴
    m = re.search(r"/v/(\d{4})(\d{2})(\d{2})", url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"[/=](\d{4})(\d{2})(\d{2})", url)
    if m and 2000 <= int(m.group(1)) <= 2026:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # 2. HTML 메타 태그
    for meta_prop in ["article:published_time", "og:regDate", "pubdate", "sailthru.date", "date"]:
        meta = soup.select_one(f"meta[property='{meta_prop}'], meta[name='{meta_prop}']")
        if meta and meta.get("content"):
            dm = re.search(r"(20[0-2][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", meta["content"])
            if dm:
                return f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"

    # 3. 날짜 표시 엘리먼트
    for sel in [
        ".media_end_head_info_datestamp_time", "._ARTICLE_DATE_TIME",
        "span.num_date", "span.data_info", "span.date", ".dates",
        ".byline", ".article_info", ".txt_info", "div.date_area"
    ]:
        elem = soup.select_one(sel)
        if elem:
            dm = re.search(r"(20[0-2][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", elem.get_text())
            if dm:
                return f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"

    # 4. 본문 시작부 1000자 내 탐색
    dm = re.search(r"(20[0-2][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})[일자]?", soup.get_text()[:1000])
    if dm:
        return f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"

    # 5. date_hint 유효성
    if date_hint and len(date_hint) >= 4:
        return date_hint[:10]

    return ""

def extract_body(soup: BeautifulSoup, url: str) -> str:
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()

    for cls_name in ["media_end_linked", "newsct_footer", "byline", "share", "reply", "comment", "copyright", "recommend", "footer_info", "sns_wrap", "btn_wrap"]:
        for div in soup.find_all(class_=lambda c: c and cls_name in str(c).lower()):
            div.decompose()

    # Daum
    if "daum.net" in url:
        b = soup.select_one("section.article_view, div.article_view, #mArticle, div.alex-target")
        if b:
            return b.get_text(separator="\n")

    # Edaily
    if "edaily.co.kr" in url:
        b = soup.select_one("#article_body, .news_body, div.article_view, div.view_con")
        if b:
            return b.get_text(separator="\n")

    # Naver
    if "news.naver.com" in url:
        b = soup.find("div", id="newsct_article") or soup.find("div", id="articleBodyContents") or soup.find("article")
        if b:
            return b.get_text(separator="\n")

    # Journalist
    if "journalist.or.kr" in url:
        b = soup.find("div", id="article-view-content-div") or soup.find("div", class_="article-body")
        if b:
            return b.get_text(separator="\n")

    art = soup.find("article") or soup.find("main") or soup.find("div", id="article_body") or soup.find("div", class_="article_body")
    if art:
        return art.get_text(separator="\n")

    return soup.get_text(separator="\n")

extracted_records: List[Dict[str, Any]] = []

def process_art(art_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    u = art_info["url"]
    t = art_info["title"]
    dh = art_info.get("date_hint", "")
    try:
        r = requests.get(u, headers=headers, timeout=5, verify=False)
        if r.status_code != 200:
            return []
        if "euc-kr" in r.text[:1000].lower() or "cp949" in r.text[:1000].lower():
            r.encoding = 'euc-kr'
        else:
            r.encoding = r.apparent_encoding or 'utf-8'

        soup = BeautifulSoup(r.text, "html.parser")
        pub_d = extract_exact_date(u, soup, dh)
        if not pub_d or pub_d.startswith("2026-11-17"):  # 허위 오늘 날짜 차단
            return []
        
        body = extract_body(soup, u)
        if not body or len(body) < 30:
            return []

        recs = PersonnelRegexParser.parse_article_text(
            article_text=body,
            default_media="이데일리",
            article_title=t,
            pub_date=pub_d,
            article_url=u
        )

        valid_recs = []
        for rec in recs:
            med = normalize_media_name(rec.get("media", ""))
            if "이데일리" in med:
                rec["media"] = "이데일리"
                if not is_excluded_department(rec.get("dept", "")) and rec.get("name") and len(rec["name"]) >= 2:
                    valid_recs.append(rec)
        return valid_recs
    except Exception:
        return []

print(f"총 {len(all_articles)}개 기사 본문 병렬 다운로드 및 파싱 실행 (최대 16 워커)...")
with ThreadPoolExecutor(max_workers=16) as pool:
    futures = [pool.submit(process_art, a) for a in all_articles.values()]
    done_cnt = 0
    for fut in as_completed(futures):
        res = fut.result()
        if res:
            extracted_records.extend(res)
        done_cnt += 1
        if done_cnt % 50 == 0 or done_cnt == len(all_articles):
            print(f"  [{done_cnt}/{len(all_articles)}] 처리 완료... 현재 추출 레코드: {len(extracted_records)}건")

print(f"\n=======================================================")
print(f"총 추출된 유효 이데일리 발령 레코드: {len(extracted_records)}건")
print(f"=======================================================")

# [3단계] DB 및 archive_seed.json.gz에 병합 영속화
print("\n=== [3단계] 데이터베이스 및 마스터 시드 파일에 안전 병합 ===")
engine = PersonnelTrackerEngine()
db = MediaDatabase()

# 기존 DB와 병합 처리
res_summary = engine.process_records(extracted_records, append=True)
print(f"DB 병합 결과: 언론인 마스터 {res_summary['unique_reporters']}명, 이직 감지 {res_summary['job_changes_detected']}건")

# 마스터 시드(archive_seed.json.gz) 동기화
all_db_records = db.get_all_raw_records()
import gzip
seed_path = os.path.join(PROJECT_ROOT, "data", "archive_seed.json.gz")
with gzip.open(seed_path, "wt", encoding="utf-8") as f:
    json.dump([dict(r) for r in all_db_records], f, ensure_ascii=False, indent=2)

print(f"마스터 시드 갱신 완료: 총 {len(all_db_records):,}건 저장 ({seed_path})")

# [4단계] 이데일리 연도별 커버리지 및 건설부동산 부장 확인
print("\n=== [4단계] 이데일리 백필 결과 검증 ===")
with db.get_connection() as conn:
    c = conn.cursor()
    c.execute("""
        SELECT substr(pub_date, 1, 4) as yr, count(*)
        FROM personnel_records
        WHERE media = '이데일리'
        GROUP BY yr
        ORDER BY yr
    """)
    rows = c.fetchall()
    print("이데일리 연도별 레코드:")
    for yr, cnt in rows:
        print(f"  {yr}년: {cnt}건")

    print("\n이데일리 건설부동산 부장 발령 연대기:")
    c.execute("""
        SELECT pub_date, media, dept, rank_title, name, action_type, article_title
        FROM personnel_records
        WHERE media = '이데일리' AND dept LIKE '%건설부동산%' AND rank_title LIKE '%부장%'
        ORDER BY pub_date DESC
    """)
    for r in c.fetchall():
        print(f"  {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5]} | {r[6]}")
