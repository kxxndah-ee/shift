"""
scripts/backfill_all_mentioned_media.py
======================================
주요 일간지·경제지 외 기사에서 언급되는 모든 언론사(전국 지역 일간지, 지역 방송사,
경제/IT/산업 전문지, 인터넷 언론사, 주간지, 종교/특수 매체 등)의 과거 22개년(2005~2026)
인사 이동 데이터를 전수 수집 및 분석하여 로컬 DB 및 마스터 시드(data/archive_seed.json.gz)에 영구 구축합니다.
"""

import os
import sys
import time
import random
import re
import html
import sqlite3
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
    is_personnel_article_title,
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

# 주요 매체 외 언급되는 전국 모든 언론사 리스트 (대표명, 탐색용 키워드 목록)
TARGET_MENTIONED_MEDIA: List[Tuple[str, List[str]]] = [
    # 1. 전국 지역 종합 일간지 (전국 17개 시도)
    ("부산일보", ["부산일보"]),
    ("국제신문", ["국제신문"]),
    ("매일신문", ["매일신문"]),
    ("영남일보", ["영남일보"]),
    ("대구일보", ["대구일보"]),
    ("대구신문", ["대구신문"]),
    ("경북일보", ["경북일보"]),
    ("경북도민일보", ["경북도민일보"]),
    ("경북매일", ["경북매일"]),
    ("강원일보", ["강원일보"]),
    ("강원도민일보", ["강원도민일보"]),
    ("대전일보", ["대전일보"]),
    ("중도일보", ["중도일보"]),
    ("충청투데이", ["충청투데이"]),
    ("충청일보", ["충청일보"]),
    ("동양일보", ["동양일보"]),
    ("중부매일", ["중부매일"]),
    ("충북일보", ["충북일보"]),
    ("광주일보", ["광주일보"]),
    ("전남일보", ["전남일보"]),
    ("전남도민일보", ["전남도민일보"]),
    ("광남일보", ["광남일보"]),
    ("남도일보", ["남도일보"]),
    ("무등일보", ["무등일보"]),
    ("광주매일", ["광주매일"]),
    ("전북일보", ["전북일보"]),
    ("전북도민일보", ["전북도민일보"]),
    ("전라일보", ["전라일보"]),
    ("새전북신문", ["새전북신문"]),
    ("경남신문", ["경남신문"]),
    ("경남도민일보", ["경남도민일보"]),
    ("경남일보", ["경남일보"]),
    ("제민일보", ["제민일보"]),
    ("한라일보", ["한라일보"]),
    ("제주일보", ["제주일보"]),
    ("경기일보", ["경기일보"]),
    ("경인일보", ["경인일보"]),
    ("중부일보", ["중부일보"]),
    ("인천일보", ["인천일보"]),
    ("기호일보", ["기호일보"]),
    ("경기신문", ["경기신문"]),
    ("울산매일", ["울산매일"]),
    ("경상일보", ["경상일보"]),
    ("울산신문", ["울산신문"]),

    # 2. 지역 방송 및 지역 민방 / 지역 MBC
    ("KNN", ["KNN"]),
    ("TBC", ["TBC"]),
    ("kbc광주방송", ["kbc"]),
    ("TJB대전방송", ["TJB"]),
    ("UBC울산방송", ["UBC"]),
    ("JTV전주방송", ["JTV"]),
    ("CJB청주방송", ["CJB"]),
    ("G1방송", ["G1"]),
    ("JIBS제주방송", ["JIBS"]),
    ("OBS경인TV", ["OBS"]),
    ("부산MBC", ["부산MBC"]),
    ("대구MBC", ["대구MBC"]),
    ("광주MBC", ["광주MBC"]),
    ("대전MBC", ["대전MBC"]),
    ("전주MBC", ["전주MBC"]),
    ("MBC경남", ["MBC경남"]),
    ("춘천MBC", ["춘천MBC"]),
    ("제주MBC", ["제주MBC"]),

    # 3. 경제지, IT 전문지, 산업 전문지
    ("아주경제", ["아주경제"]),
    ("아시아투데이", ["아시아투데이"]),
    ("이투데이", ["이투데이"]),
    ("뉴스토마토", ["뉴스토마토", "토마토"]),
    ("비즈워치", ["비즈워치", "비즈니스워치"]),
    ("비즈한국", ["비즈한국"]),
    ("더벨", ["더벨"]),
    ("딜사이트", ["딜사이트"]),
    ("인포스탁데일리", ["인포스탁데일리"]),
    ("이코노미스트", ["이코노미스트"]),
    ("포브스코리아", ["포브스코리아"]),
    ("매경이코노미", ["매경이코노미"]),
    ("한경비즈니스", ["한경비즈니스"]),
    ("에너지경제", ["에너지경제"]),
    ("글로벌이코노믹", ["글로벌이코노믹"]),
    ("뉴스웨이", ["뉴스웨이"]),
    ("한국정경신문", ["한국정경신문"]),
    ("스마트에프엔", ["스마트에프엔"]),
    ("메트로신문", ["메트로신문"]),
    ("신아일보", ["신아일보"]),
    ("브릿지경제", ["브릿지경제"]),
    ("조세일보", ["조세일보"]),
    ("대한경제", ["대한경제", "건설경제"]),
    ("지디넷코리아", ["지디넷"]),
    ("블로터", ["블로터"]),
    ("테크M", ["테크M"]),
    ("디지털데일리", ["디지털데일리"]),
    ("보안뉴스", ["보안뉴스"]),
    ("데일리시큐", ["데일리시큐"]),
    ("의학신문", ["의학신문"]),
    ("메디칼타임즈", ["메디칼타임즈"]),
    ("청년의사", ["청년의사"]),
    ("약업신문", ["약업신문"]),
    ("데일리팜", ["데일리팜"]),
    ("식품저널", ["식품저널"]),
    ("농민신문", ["농민신문"]),
    ("한국농어민신문", ["한국농어민신문"]),
    ("전기신문", ["전기신문"]),
    ("정보통신신문", ["정보통신신문"]),
    ("국토일보", ["국토일보"]),
    ("한국대학신문", ["한국대학신문"]),
    ("교수신문", ["교수신문"]),

    # 4. 인터넷 종합지 및 주간지 / 시사지
    ("데일리안", ["데일리안"]),
    ("프레시안", ["프레시안"]),
    ("오마이뉴스", ["오마이뉴스"]),
    ("미디어오늘", ["미디어오늘"]),
    ("기자협회보", ["기자협회보"]),
    ("PD저널", ["PD저널"]),
    ("노컷뉴스", ["노컷뉴스"]),
    ("더팩트", ["더팩트"]),
    ("더구루", ["더구루"]),
    ("뉴스핌", ["뉴스핌"]),
    ("미디어펜", ["미디어펜"]),
    ("EBN", ["EBN"]),
    ("뉴스포스트", ["뉴스포스트"]),
    ("시사저널", ["시사저널"]),
    ("시사IN", ["시사IN", "시사인"]),
    ("일요신문", ["일요신문"]),
    ("주간조선", ["주간조선"]),
    ("주간동아", ["주간동아"]),
    ("한겨레21", ["한겨레21"]),
    ("주간경향", ["주간경향"]),
    ("시사주간", ["시사주간"]),

    # 5. 스포츠 및 연예 전문지
    ("스포츠조선", ["스포츠조선"]),
    ("스포츠동아", ["스포츠동아"]),
    ("스포츠서울", ["스포츠서울"]),
    ("스포츠경향", ["스포츠경향"]),
    ("일간스포츠", ["일간스포츠"]),
    ("OSEN", ["OSEN"]),
    ("마이데일리", ["마이데일리"]),
    ("스타뉴스", ["스타뉴스"]),
    ("스포티비뉴스", ["스포티비뉴스"]),

    # 6. 종교 / 특수 방송 및 신문
    ("BBS불교방송", ["BBS", "불교방송"]),
    ("CPBC가톨릭평화방송", ["CPBC", "평화방송"]),
    ("CBS기독교방송", ["CBS", "기독교방송"]),
    ("국방일보", ["국방일보"])
]


def clean_up_existing_bogus_media(db: MediaDatabase):
    """기존 DB에 부서명/일반명사로 잘못 등재된 가짜 매체 레코드를 정제 및 재분류합니다."""
    bogus_medias = [
        "미디어", "방송", "멀티미디어", "경제", "뉴미디어", "미디어연구소",
        "미디어전략연구소", "미래미디어", "출판미디어", "방송뉴스",
        "경영본부 주간 이완성", "전략기획실 미디어기술연구소"
    ]
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute(f"""
            SELECT COUNT(*) FROM personnel_records 
            WHERE media IN ({','.join(['?']*len(bogus_medias))})
        """, bogus_medias)
        bogus_cnt = c.fetchone()[0]
        if bogus_cnt > 0:
            print(f"[정제 조치] 기존 오분류된 가짜 매체 레코드 {bogus_cnt:,}건 발견 -> 정밀 재정제 및 삭제 수행")
            c.execute(f"""
                DELETE FROM personnel_records 
                WHERE media IN ({','.join(['?']*len(bogus_medias))})
            """, bogus_medias)
            conn.commit()
            print("[정제 조치 완료] 가짜 매체 데이터베이스 정리 완료")


def crawl_articles_for_query(query: str, year: int, seen_urls: Set[str], max_pages: int = 2) -> List[Dict[str, Any]]:
    """네이버 뉴스에서 특정 질의 및 연도 범위로 검색하여 기사 메타 목록을 수집합니다."""
    articles = []
    ua = random.choice(USER_AGENTS)
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Upgrade-Insecure-Requests": "1"
    }

    start_indices = [1] if max_pages == 1 else [1, 11]
    for start_idx in start_indices:
        start_param = f"&start={start_idx}" if start_idx > 1 else ""
        url = (
            f"https://search.naver.com/search.naver?where=news&query={quote(query)}"
            f"&sort=0&ds={year}.01.01&de={year}.12.31&nso=so:r,p:from{year}0101to{year}1231,a:all{start_param}"
        )
        try:
            time.sleep(random.uniform(0.10, 0.25))
            resp = requests.get(url, headers=headers, timeout=4.5, verify=False)
            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.content, "html.parser")
            tit_anchors = soup.select("a.news_tit, a.W5fYO2_0xMDeLrlx")
            if not tit_anchors:
                for a in soup.find_all("a"):
                    h = a.get("href", "")
                    t = a.get_text(strip=True).replace("새 창 열림", "").replace("새창열림", "").strip()
                    if not h or not t or "keep.naver.com" in h or "/press/" in h:
                        continue
                    if is_personnel_article_title(t, url=h):
                        tit_anchors.append(a)

            for tit_a in tit_anchors:
                raw_tit = tit_a.get_text(strip=True).replace("새 창 열림", "").replace("새창열림", "").strip()
                art_url = tit_a.get("href", "")
                if not art_url or art_url in seen_urls:
                    continue

                if is_personnel_article_title(raw_tit, url=art_url):
                    seen_urls.add(art_url)
                    articles.append({
                        "url": art_url,
                        "title": raw_tit,
                        "year": year
                    })
        except Exception:
            pass

    return articles


def fetch_and_parse_article(art: Dict[str, Any], default_media: str) -> List[Dict[str, Any]]:
    """기사 본문을 추출하고 인사 발령 레코드를 파싱합니다."""
    url = art["url"]
    title = art["title"]
    year = art["year"]

    try:
        time.sleep(random.uniform(0.08, 0.20))
        fetched = OnlineNewsTracker.fetch_full_text(url)
        body = fetched.get("text", "")
        pub_date = fetched.get("pub_date") or f"{year}-01-01"

        if not body or len(body.strip()) < 10:
            return []

        recs = PersonnelRegexParser.parse_article_text(
            article_text=body,
            default_media=default_media,
            article_title=title,
            pub_date=pub_date,
            article_url=url
        )

        valid_recs = []
        for r in recs:
            med = r.get("media", "")
            dept = r.get("dept", "")
            if is_media_organization(med) and not is_excluded_department(dept):
                r["pub_date"] = pub_date
                r["article_title"] = title
                r["article_url"] = url
                valid_recs.append(r)

        return valid_recs
    except Exception:
        return []


def main():
    print("=" * 75)
    print("전국 모든 언론사(지역 일간지/방송/전문지/인터넷지) 22개년 인사 아카이브 마스터 구축")
    print("=" * 75)

    db = MediaDatabase("media_tracker.db")

    # 1. 기존 DB의 가짜 매체 레코드 정제
    clean_up_existing_bogus_media(db)

    # 2. 수집 대상 매체 및 누락 연도 산출
    # 2005~2026년 중 데이터가 5건 미만인 연도를 자동 선별
    all_years = list(range(2005, 2027))
    tasks_to_run: List[Tuple[str, str, int]] = []

    with db.get_connection() as conn:
        c = conn.cursor()
        for media_key, aliases in TARGET_MENTIONED_MEDIA:
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
                cnt = counts.get(str(y), 0)
                if cnt < 5:
                    tasks_to_run.append((media_key, aliases[0], y))

    print(f"총 수집 대상 미완료 태스크: {len(tasks_to_run)}건")

    # 3. 추가로 종합 언론계 인사 기사([인사] 종합, [인사] 언론계 등) 연도별 수집
    roundup_queries = [
        "[인사] 종합",
        "[인사] 언론계",
        "[인사] 지역신문",
        "[인사] 방송사",
        "[인사] 신문사"
    ]
    roundup_tasks = []
    for y in all_years:
        for rq in roundup_queries:
            roundup_tasks.append((rq, y))

    print(f"종합 언론계 인사 라운드업 태스크: {len(roundup_tasks)}건 추가")

    seen_urls: Set[str] = set()
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT DISTINCT article_url FROM personnel_records WHERE article_url IS NOT NULL")
        for (u,) in c.fetchall():
            if u:
                seen_urls.add(u)

    print(f"기존 기수집 URL {len(seen_urls):,}건 중복 배제 적용")

    # 4. 멀티스레드 병렬 크롤링 실행
    new_candidate_articles: List[Tuple[Dict[str, Any], str]] = []

    print("\n--- 1단계: 언론사별 및 종합 인사 기사 링크 병렬 수집 중 ---")
    start_time = time.time()

    def process_media_year_task(task_item):
        m_key, query_word, yr = task_item
        queries = [
            f'"{query_word}" [인사]',
            f'"{query_word}" "부장" "인사"',
            f'[인사] {query_word}'
        ]
        found = []
        for q in queries:
            arts = crawl_articles_for_query(q, yr, seen_urls, max_pages=1)
            for a in arts:
                found.append((a, m_key))
            if len(found) >= 4:
                break
        return found

    def process_roundup_task(task_item):
        rq, yr = task_item
        arts = crawl_articles_for_query(rq, yr, seen_urls, max_pages=1)
        return [(a, "종합") for a in arts]

    # 태스크 혼합 및 실행 (매체별 태스크 + 라운드업 태스크)
    all_crawl_jobs = []
    for t in tasks_to_run:
        all_crawl_jobs.append(("media", t))
    for t in roundup_tasks:
        all_crawl_jobs.append(("roundup", t))

    # 랜덤 셔플로 서버 부하 분산
    random.shuffle(all_crawl_jobs)

    completed_jobs = 0
    with ThreadPoolExecutor(max_workers=6) as executor:
        future_to_job = {}
        for j_type, task_data in all_crawl_jobs:
            if j_type == "media":
                f = executor.submit(process_media_year_task, task_data)
            else:
                f = executor.submit(process_roundup_task, task_data)
            future_to_job[f] = task_data

        for f in as_completed(future_to_job):
            completed_jobs += 1
            try:
                res_arts = f.result()
                if res_arts:
                    new_candidate_articles.extend(res_arts)
            except Exception:
                pass

            if completed_jobs % 100 == 0 or completed_jobs == len(all_crawl_jobs):
                print(f"진행률: [{completed_jobs}/{len(all_crawl_jobs)}] 완료 | 수집된 신규 인사 기사: {len(new_candidate_articles):,}건")

    print(f"\n1단계 완료! 총 {len(new_candidate_articles):,}건의 신규 유효 인사 기사 확보 ({time.time() - start_time:.1f}초)")

    # 5. 본문 추출 및 인사 발령 데이터 정밀 파싱
    print("\n--- 2단계: 기사 본문 추출 및 인사이동 레코드 파싱 중 ---")
    all_parsed_records: List[Dict[str, Any]] = []
    parse_start = time.time()

    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_parse = {
            executor.submit(fetch_and_parse_article, art_info, def_media): art_info
            for art_info, def_media in new_candidate_articles
        }

        parsed_cnt = 0
        for f in as_completed(future_to_parse):
            parsed_cnt += 1
            try:
                records = f.result()
                if records:
                    all_parsed_records.extend(records)
            except Exception:
                pass

            if parsed_cnt % 50 == 0 or parsed_cnt == len(new_candidate_articles):
                print(f"파싱 진행률: [{parsed_cnt}/{len(new_candidate_articles)}] 완료 | 추출된 발령 레코드: {len(all_parsed_records):,}건")

    print(f"\n2단계 완료! 신규 발령 레코드 총 {len(all_parsed_records):,}건 파싱 완료 ({time.time() - parse_start:.1f}초)")

    # 6. 엔진 클러스터링 및 데이터베이스 영구 적재
    print("\n--- 3단계: PersonnelTrackerEngine 클러스터링 및 DB 적재 ---")
    if all_parsed_records:
        engine = PersonnelTrackerEngine("media_tracker.db")
        engine_res = engine.process_records(all_parsed_records, append=True)
        print(f"클러스터링 결과: {engine_res}")
    else:
        print("신규 추가 레코드 없음")

    # 7. 마스터 시드 data/archive_seed.json.gz 동기화
    print("\n--- 4단계: data/archive_seed.json.gz 마스터 시드 직렬화 동기화 ---")
    seed_cnt = db.dump_seed_data("data/archive_seed.json.gz")
    print(f"마스터 시드 동기화 완료: {seed_cnt:,}건 압축 저장")

    # 8. 최종 현황 출력
    stats = db.get_stats()
    print("\n" + "=" * 75)
    print("최종 전국 언론사 인사 데이터베이스 현황")
    print("=" * 75)
    print(f"• 총 인사 발령 레코드: {stats['records']:,}건")
    print(f"• 고유 언론인 프로필: {stats['reporters']:,}명")
    print(f"• 탐지된 이직/변동 내역: {stats['changes']:,}건")
    print(f"• 수집된 전체 언론사 수: {stats['media_count']:,}개사")
    print("=" * 75)


if __name__ == "__main__":
    main()
