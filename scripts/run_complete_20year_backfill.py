"""
scripts/run_complete_20year_backfill.py
=======================================
전국 35개 주요 언론사(일간지, 경제지, 방송사, 통신사)의 2005~2026년(22개년)
누락 연도 인사 발령 기사를 포털 및 언론사 아카이브에서 전수 수집하여 DB에 영구 적재하고
마스터 시드(data/archive_seed.json.gz)를 직렬화 갱신합니다.
"""

import os
import sys
import time
import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Set, Tuple
import urllib3

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
    OnlineNewsTracker,
    is_media_organization,
    is_excluded_department,
    normalize_media_name,
    KNOWN_MEDIA_NAMES,
    MEDIA_GROUP_ALIASES
)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/124.0.0.0 Safari/537.36"
]

TARGET_MEDIA_LIST = [
    # 1. 주요 일간지
    ("조선일보", ["조선일보"]),
    ("동아일보", ["동아일보"]),
    ("경향신문", ["경향신문"]),
    ("한겨레", ["한겨레", "한겨레신문"]),
    ("서울신문", ["서울신문", "대한매일"]),
    ("세계일보", ["세계일보"]),
    ("국민일보", ["국민일보"]),
    ("문화일보", ["문화일보"]),
    ("중앙일보", ["중앙일보"]),
    ("한국일보", ["한국일보"]),
    # 2. 경제지 및 IT전문지
    ("헤럴드경제", ["헤럴드경제", "헤럴드", "코리아헤럴드", "헤럴드미디어"]),
    ("매일경제", ["매일경제", "매일경제신문", "매경"]),
    ("한국경제", ["한국경제", "한국경제신문", "한경"]),
    ("서울경제", ["서울경제", "서울경제신문", "서경"]),
    ("아시아경제", ["아시아경제", "아시아경제신문", "아경"]),
    ("파이낸셜뉴스", ["파이낸셜뉴스", "파이낸셜"]),
    ("머니투데이", ["머니투데이"]),
    ("이데일리", ["이데일리"]),
    ("전자신문", ["전자신문"]),
    ("디지털타임스", ["디지털타임스", "디지털타임즈"]),
    ("아주경제", ["아주경제", "아주경제신문"]),
    # 3. 통신사
    ("연합뉴스", ["연합뉴스"]),
    ("뉴스1", ["뉴스1"]),
    ("뉴시스", ["뉴시스"]),
    # 4. 방송사
    ("KBS", ["KBS", "한국방송"]),
    ("MBC", ["MBC", "문화방송"]),
    ("SBS", ["SBS"]),
    ("YTN", ["YTN"]),
    ("연합뉴스TV", ["연합뉴스TV", "뉴스Y"]),
    ("JTBC", ["JTBC"]),
    ("MBN", ["MBN", "매일방송"]),
    ("채널A", ["채널A"]),
    ("TV조선", ["TV조선"]),
    ("OBS", ["OBS"]),
    ("CBS", ["CBS"]),
    ("BBS", ["BBS"])
]

def get_missing_media_years(db: MediaDatabase) -> List[Tuple[str, str, int]]:
    """각 언론사 및 별칭별로 2005~2026년 중 데이터가 15건 미만인 (대표매체명, 검색단어, 연도) 태스크를 반환합니다."""
    all_years = list(range(2005, 2027))
    tasks = []
    with db.get_connection() as conn:
        c = conn.cursor()
        for media_key, aliases in TARGET_MEDIA_LIST:
            clause = " OR ".join(["media LIKE ?" for _ in aliases])
            params = [f"%{a}%" for a in aliases]
            c.execute(f"""
                SELECT SUBSTR(pub_date, 1, 4) as yr, COUNT(*)
                FROM personnel_records
                WHERE {clause}
                GROUP BY yr
            """, params)
            counts = dict(c.fetchall())
            for y in all_years:
                if counts.get(str(y), 0) < 15:
                    for a in aliases:
                        tasks.append((media_key, a, y))
    return tasks

def scan_single_outlet_year(media_key: str, outlet: str, yr: int, seen_urls: Set[str]) -> List[Dict[str, Any]]:
    """특정 언론사/연도에 대해 네이버 뉴스 검색을 실행합니다."""
    items = []
    queries = [
        f'"{outlet}" [인사]',
        f'[인사] {outlet}'
    ]
    if not outlet.endswith(("신문", "방송", "뉴스", "통신", "일보", "타임스", "타임즈")):
        queries.append(f'"{outlet}신문" [인사]')

    ds = f"{yr}.01.01"
    de = f"{yr}.12.31"
    p_from_to = f"p:from{yr}0101to{yr}1231"

    for q in queries:
        for start_idx in [1, 11]:
            start_param = f"&start={start_idx}" if start_idx > 1 else ""
            url = f"https://search.naver.com/search.naver?where=news&query={quote(q)}&sort=0&ds={ds}&de={de}&nso=so:r,{p_from_to},a:all{start_param}"
            ua = random.choice(USER_AGENTS)
            headers = {
                "User-Agent": ua,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
            }
            try:
                resp = requests.get(url, headers=headers, timeout=4.5, verify=False)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, "html.parser")
                    tit_elems = soup.select("a.news_tit, a.W5fYO2_0xMDeLrlx")
                    for a in tit_elems:
                        h = a.get("href", "")
                        t = a.get_text(strip=True).replace("새 창 열림", "").replace("새창열림", "").strip()
                        if not h or not t or h in seen_urls:
                            continue
                        if not any(k in t for k in ["[인사]", "<인사>", "【인사】", "인사"]):
                            continue
                        seen_urls.add(h)
                        items.append({
                            "url": h,
                            "title": t,
                            "media_key": media_key,
                            "outlet": outlet,
                            "year": yr
                        })
                time.sleep(0.12)
            except Exception:
                pass
    return items

def process_article_worker(art: Dict[str, Any]) -> List[Dict[str, Any]]:
    u = art["url"]
    t = art["title"]
    yr = art["year"]
    m_key = art["media_key"]
    try:
        body_res = OnlineNewsTracker.fetch_full_text(u)
        content = body_res.get("text", "")
        if not content or len(content) < 20:
            return []
        pub_date = body_res.get("pub_date") or f"{yr}-12-31"

        recs = PersonnelRegexParser.parse_article_text(
            article_title=t,
            article_text=content,
            article_url=u,
            pub_date=pub_date,
            default_media=m_key
        )
        valid = []
        for r in recs:
            if is_media_organization(r.get("media", "")) and not is_excluded_department(r.get("dept", "")):
                valid.append(r)
        return valid
    except Exception:
        return []

def run_main():
    start_time = time.time()
    db = MediaDatabase()
    engine = PersonnelTrackerEngine("media_tracker.db")

    print("=" * 75)
    print(" [전국 주요 35개 언론사 과거 22개년(2005~2026) 아카이브 전수 정밀 수집 및 DB 동기화] ")
    print("=" * 75)

    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT article_url FROM personnel_records WHERE article_url != ''")
        seen_urls = {r[0] for r in c.fetchall()}
    print(f"기존 DB 수집 기사 URL: {len(seen_urls)}건 (중복 방지 캐시 장착)")

    tasks = get_missing_media_years(db)
    print(f"누락/부족 연도(15건 미만) 정밀 타깃 태스크: 총 {len(tasks)}개 탐색 태스크 산출\n")

    candidate_articles: List[Dict[str, Any]] = []
    
    # 8개 스레드로 네이버 뉴스 병렬 크롤링 (각 요청 간 0.12s 딜레이 및 UA 로테이션)
    with ThreadPoolExecutor(max_workers=8) as executor:
        futs = {executor.submit(scan_single_outlet_year, m_key, out, yr, seen_urls): (m_key, out, yr) for m_key, out, yr in tasks}
        done = 0
        for f in as_completed(futs):
            done += 1
            res = f.result()
            if res:
                candidate_articles.extend(res)
            if done % 50 == 0 or done == len(tasks):
                print(f"  [스캔] {done}/{len(tasks)} 완료 (신규 기사 {len(candidate_articles)}건 발견)")

    print(f"\n-> 총 {len(candidate_articles)}건 신규 발령 기사 확보. 병렬 본문 수집 및 파싱 시작...")

    all_new_records = []
    with ThreadPoolExecutor(max_workers=14) as parse_exec:
        parse_futs = [parse_exec.submit(process_article_worker, art) for art in candidate_articles]
        p_done = 0
        for pf in as_completed(parse_futs):
            p_done += 1
            recs = pf.result()
            if recs:
                all_new_records.extend(recs)
            if p_done % 50 == 0 or p_done == len(candidate_articles):
                print(f"  [파싱] {p_done}/{len(candidate_articles)} 완료 (누적 레코드: {len(all_new_records)}건)")

    print(f"\nDB 영구 적재: {len(all_new_records)}건 인사 레코드 중복 필터링 및 삽입...")
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT pub_date, media, name, dept, rank_title FROM personnel_records")
        existing_keys = set(cursor.fetchall())

        to_insert = []
        for r in all_new_records:
            k = (r.get("pub_date"), r.get("media"), r.get("name"), r.get("dept"), r.get("rank_title"))
            if k not in existing_keys:
                existing_keys.add(k)
                to_insert.append((
                    r.get("pub_date"), r.get("media"), r.get("dept"), r.get("domain", ""),
                    r.get("rank_title"), r.get("rank_level", 2), r.get("name"), r.get("action_type", ""),
                    r.get("email", ""), r.get("raw_text", ""), r.get("reporter_id", ""),
                    r.get("article_title", ""), r.get("article_url", "")
                ))
        if to_insert:
            cursor.executemany("""
                INSERT INTO personnel_records (
                    pub_date, media, dept, domain, rank_title, rank_level, name,
                    action_type, email, raw_text, reporter_id, article_title, article_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, to_insert)
            conn.commit()
            print(f"  -> 성공: {len(to_insert)}건 신규 인사 레코드 추가 완료!")
        else:
            print("  -> 추가할 신규 레코드 없음")

    print("\n언론인 프로필 및 이직 연대기 타임라인 전체 동기화...")
    engine.process_records([])

    seed_path = os.path.join(PROJECT_ROOT, "data", "archive_seed.json.gz")
    print(f"\n마스터 아카이브 시드 영구 압축 갱신 -> {seed_path}...")
    db.dump_seed_data(seed_path)

    elapsed = time.time() - start_time
    print(f"\n전국 35개 언론사 2005~2026 전수 아카이브 백필 완료! (총 {elapsed:.1f}초 소요)")

if __name__ == "__main__":
    run_main()
