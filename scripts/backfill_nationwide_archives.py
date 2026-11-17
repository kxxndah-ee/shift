"""
scripts/backfill_nationwide_archives.py
=======================================
전국 주요 언론사(일간지, 경제지, 방송사, 통신사)의 2005~2026년(22개년)
누락 연도 인사 기사를 포털 및 언론사 아카이브에서 전수 수집하여 DB에 영구 적재하고
마스터 시드(data/archive_seed.json.gz)를 갱신합니다.
"""

import os
import sys
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Set
import urllib3

urllib3.disable_warnings()

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# 프로젝트 루트 임포트 경로 추가
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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}
session = requests.Session()
session.headers.update(HEADERS)

# 주요 언론사 및 별칭 목록
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
    # 2. 경제지 및 IT전문지
    ("매일경제", ["매일경제", "매일경제신문", "매경"]),
    ("한국경제", ["한국경제", "한국경제신문", "한경"]),
    ("서울경제", ["서울경제", "서울경제신문", "서경"]),
    ("헤럴드경제", ["헤럴드경제", "헤럴드", "코리아헤럴드", "헤럴드미디어"]),
    ("아시아경제", ["아시아경제", "아시아경제신문", "아경"]),
    ("파이낸셜뉴스", ["파이낸셜뉴스", "파이낸셜"]),
    ("머니투데이", ["머니투데이"]),
    ("이데일리", ["이데일리"]),
    ("전자신문", ["전자신문"]),
    ("디지털타임스", ["디지털타임스", "디지털타임즈"]),
    ("아주경제", ["아주경제", "아주경제신문"]),
    # 3. 통신사
    ("연합뉴스", ["연합뉴스"]),
    ("뉴시스", ["뉴시스"]),
    ("뉴스1", ["뉴스1"]),
    # 4. 방송사
    ("KBS", ["KBS", "한국방송"]),
    ("MBC", ["MBC", "문화방송"]),
    ("SBS", ["SBS"]),
    ("YTN", ["YTN"]),
    ("연합뉴스TV", ["연합뉴스TV", "뉴스Y"]),
    ("JTBC", ["JTBC"]),
    ("MBN", ["MBN", "매일방송"]),
    ("채널A", ["채널A"]),
    ("TV조선", ["TV조선"])
]

def get_missing_years_per_media(db: MediaDatabase) -> Dict[str, List[int]]:
    """각 언론사별로 2005~2026년 중 데이터가 비어있거나 부족한 연도를 추출합니다."""
    all_years = list(range(2005, 2027))
    missing_map = {}
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
            # 5건 미만인 연도는 누락 연도로 판별하여 전수 수집
            missing_years = [y for y in all_years if counts.get(str(y), 0) < 5]
            missing_map[media_key] = missing_years
    return missing_map

def scan_outlet_year_articles(outlet: str, yr: int, seen_urls: Set[str]) -> List[Dict[str, Any]]:
    """특정 언론사의 특정 연도에 대해 네이버 뉴스 검색을 실행하여 인사 기사 후보를 추출합니다."""
    items = []
    queries = [f'"{outlet}" "[인사]"', f'[인사] {outlet}']
    if not outlet.endswith(("신문", "방송", "뉴스", "통신", "일보", "타임스", "타임즈")):
        queries.append(f'"{outlet}신문" [인사]')

    ds = f"{yr}.01.01"
    de = f"{yr}.12.31"
    p_from_to = f"p:from{yr}0101to{yr}1231"

    for q in queries:
        for start_idx in [1, 11]:
            start_param = f"&start={start_idx}" if start_idx > 1 else ""
            url = f"https://search.naver.com/search.naver?where=news&query={quote(q)}&sort=0&ds={ds}&de={de}&nso=so:r,{p_from_to},a:all{start_param}"
            try:
                resp = session.get(url, timeout=3.5, verify=False)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, "html.parser")
                    tit_elems = soup.select(".news_tit, a.W5fYO2_0xMDeLrlx, a[href*='articleView'], a[href*='article']")
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
                            "outlet": outlet,
                            "year": yr
                        })
            except Exception:
                pass
    return items

def process_single_article(art: Dict[str, Any]) -> List[Dict[str, Any]]:
    """기사 본문을 수집하고 인사 레코드로 정밀 파싱합니다."""
    u = art["url"]
    t = art["title"]
    yr = art["year"]
    out = art["outlet"]
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
            default_media=out
        )
        valid = []
        for r in recs:
            if is_media_organization(r.get("media", "")) and not is_excluded_department(r.get("dept", "")):
                valid.append(r)
        return valid
    except Exception:
        return []

def run_nationwide_backfill():
    start_time = time.time()
    db = MediaDatabase()
    engine = PersonnelTrackerEngine("media_tracker.db")

    print("=" * 70)
    print(" [전국 주요 32개 언론사 2005~2026년 과거 22개년 아카이브 정밀 전수 백필] ")
    print("=" * 70)

    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT article_url FROM personnel_records WHERE article_url != ''")
        seen_urls = {r[0] for r in c.fetchall()}
    print(f"기존 DB 수집 완료된 기사 URL: {len(seen_urls)}건 (중복 스킵 처리)")

    missing_map = get_missing_years_per_media(db)
    total_missing_slots = sum(len(yrs) for yrs in missing_map.values())
    print(f"누락 연도 정밀 분석 결과: 총 {len(missing_map)}개 매체에서 {total_missing_slots}개 매체-연도 슬롯 보충 필요\n")

    tasks = []
    for media_key, aliases in TARGET_MEDIA_LIST:
        missing_yrs = missing_map.get(media_key, [])
        for yr in missing_yrs:
            for alias in aliases:
                tasks.append((alias, yr))

    print(f"네이버 뉴스 22년 치 병렬 스캔 작업 생성: 총 {len(tasks)}개 탐색 태스크...")
    candidate_articles: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=14) as executor:
        futs = {executor.submit(scan_outlet_year_articles, out, yr, seen_urls): (out, yr) for out, yr in tasks}
        done = 0
        for f in as_completed(futs):
            done += 1
            res = f.result()
            if res:
                candidate_articles.extend(res)
            if done % 100 == 0 or done == len(tasks):
                print(f"  스캔 진행: {done}/{len(tasks)} 태스크 완료 (신규 기사 {len(candidate_articles)}건 발견)")

    print(f"\n-> 총 {len(candidate_articles)}건 신규 발령 기사 확보. 병렬 본문 수집 및 파싱 시작...")

    all_new_records = []
    with ThreadPoolExecutor(max_workers=16) as parse_exec:
        parse_futs = [parse_exec.submit(process_single_article, art) for art in candidate_articles]
        p_done = 0
        for pf in as_completed(parse_futs):
            p_done += 1
            recs = pf.result()
            if recs:
                all_new_records.extend(recs)
            if p_done % 100 == 0 or p_done == len(candidate_articles):
                print(f"  본문 파싱 진행: {p_done}/{len(candidate_articles)} 기사 처리 (누적 레코드: {len(all_new_records)}건)")

    print(f"\nDB 반영: {len(all_new_records)}건 인사 레코드 중복 필터링 및 영구 삽입...")
    inserted_count = 0
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
            inserted_count = len(to_insert)
            print(f"  -> 성공: {inserted_count}건 신규 인사 레코드 추가 완료!")
        else:
            print("  -> 추가할 신규 레코드 없음 (이미 데이터가 최신 상태입니다)")

    print("\n기자 프로필 및 이직 연대기 타임라인 전면 재산출...")
    engine.build_all_reporter_profiles()

    print("\n마스터 아카이브 시드(data/archive_seed.json.gz) 최신 직렬화 갱신...")
    db.dump_archive_seed(os.path.join(PROJECT_ROOT, "data", "archive_seed.json.gz"))

    elapsed = time.time() - start_time
    print(f"\n전국 22년 치 아카이브 전수 백필 완료! (소요시간: {elapsed:.1f}초)")

if __name__ == "__main__":
    run_nationwide_backfill()
