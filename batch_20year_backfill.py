"""
batch_20year_backfill.py
========================
과거 20년(2006~2026) 전 언론사(주요 일간지·경제지·방송사·통신사·IT전문지·지역지 등)의
정기인사 및 발령 기사를 종합 수집하여 media_tracker.db 및 media_personnel.db에 영구 적재하고,
data/archive_seed.json.gz 시드를 최종 갱신합니다.
"""

import os
import sys
import re
import time
import json
import random
import sqlite3
import html
from datetime import datetime
from urllib.parse import quote, urljoin
import concurrent.futures
from typing import List, Dict, Any, Set, Tuple

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

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

TRACKER_DB = "media_tracker.db"
PERSONNEL_DB = "media_personnel.db"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
}

session = requests.Session()
session.headers.update(HEADERS)


def extract_media_from_title(title: str) -> str:
    m = re.search(r"[\[<【(]\s*인사\s*[\]>】)]\s*([가-힣A-Za-z0-9]+)", title)
    if m:
        return normalize_media_name(m.group(1).strip())
    for km in KNOWN_MEDIA_NAMES:
        if km in title:
            return normalize_media_name(km)
    return ""


def get_existing_urls() -> Set[str]:
    """기존 수집된 기사 URL 목록을 조회하여 중복 수집을 방지합니다."""
    urls = set()
    try:
        conn = sqlite3.connect(TRACKER_DB)
        c = conn.cursor()
        c.execute("SELECT DISTINCT article_url FROM personnel_records WHERE article_url IS NOT NULL AND article_url != ''")
        for r in c.fetchall():
            urls.add(r[0])
        conn.close()
    except Exception:
        pass

    try:
        conn = sqlite3.connect(PERSONNEL_DB)
        c = conn.cursor()
        c.execute("SELECT article_id FROM raw_articles")
        for r in c.fetchall():
            urls.add(r[0])
        conn.close()
    except Exception:
        pass

    return urls


# =========================================================================
# 1. 한국기자협회보 전수 크롤러 (전국 200여 회원 언론사 인사 망라)
# =========================================================================
def collect_journalist_articles(max_pages: int = 60, existing_urls: Set[str] = None) -> List[Dict[str, Any]]:
    print(f"\n[1/4] 한국기자협회보 인사 섹션 수집 (1~{max_pages}페이지)...")
    articles = []
    seen = set(existing_urls or [])

    for page in range(1, max_pages + 1):
        url = f"https://www.journalist.or.kr/news/section_list_all.html?sec_no=63&page={page}"
        try:
            resp = session.get(url, timeout=6, verify=False)
            if resp.status_code != 200:
                break
            soup = BeautifulSoup(resp.content, "html.parser")
            anchors = soup.select("a[href*='article.html?no=']")
            if not anchors:
                break

            for a in anchors:
                href = a.get("href", "")
                title = a.get_text(strip=True)
                if not href or not title or len(title) < 3:
                    continue
                if not any(k in title for k in ["[인사]", "<인사>", "인사"]):
                    continue
                full_url = urljoin(url, href)
                if full_url in seen:
                    continue
                seen.add(full_url)

                articles.append({
                    "url": full_url,
                    "title": title,
                    "source": "한국기자협회",
                    "date": ""
                })

            if page % 10 == 0 or page == max_pages:
                print(f"  기자협회 {page}페이지 완료 (누적 {len(articles)}건 후보 발견)")
            time.sleep(0.1)
        except Exception as e:
            print(f"  기자협회 {page}페이지 오류: {e}")
            break

    print(f"  -> 한국기자협회보 총 {len(articles)}건 발령 기사 확보")
    return articles


# =========================================================================
# 2. 중앙일보 피플 아카이브 크롤러 (1990년대~2026년 인사 보관소)
# =========================================================================
def collect_joongang_articles(max_pages: int = 60, existing_urls: Set[str] = None) -> List[Dict[str, Any]]:
    print(f"\n[2/4] 중앙일보 피플 인사 아카이브 수집 (1~{max_pages}페이지)...")
    articles = []
    seen = set(existing_urls or [])

    for page in range(1, max_pages + 1):
        url = f"https://www.joongang.co.kr/people/personnel?page={page}"
        try:
            resp = session.get(url, timeout=6, verify=False)
            if resp.status_code != 200:
                break
            soup = BeautifulSoup(resp.content, "html.parser")
            cards = soup.select(".card_body a, h2.headline a, a[href*='/article/']")
            if not cards:
                break

            for a in cards:
                href = a.get("href", "")
                title = a.get_text(strip=True)
                if "/article/" not in href or not title or len(title) < 3:
                    continue
                if not any(k in title for k in ["[인사]", "<인사>", "인사"]):
                    continue
                full_url = href if href.startswith("http") else f"https://www.joongang.co.kr{href}"
                if full_url in seen:
                    continue
                seen.add(full_url)

                articles.append({
                    "url": full_url,
                    "title": title,
                    "source": "중앙일보",
                    "date": ""
                })

            if page % 10 == 0 or page == max_pages:
                print(f"  중앙일보 {page}페이지 완료 (누적 {len(articles)}건 후보 발견)")
            time.sleep(0.1)
        except Exception as e:
            print(f"  중앙일보 {page}페이지 오류: {e}")
            break

    print(f"  -> 중앙일보 피플 총 {len(articles)}건 발령 기사 확보")
    return articles


# =========================================================================
# 3. 네이버 뉴스 20년치(2006~2026) 전 언론사 정기인사 스캔
# =========================================================================
TARGET_OUTLETS = [
    # 주요 전국 일간지
    "조선일보", "동아일보", "경향신문", "한겨레", "서울신문", "한국일보", "세계일보", "국민일보", "문화일보",
    # 주요 경제지
    "한국경제", "한국경제신문", "매일경제", "매일경제신문", "서울경제", "서울경제신문", "아시아경제", "헤럴드경제", "파이낸셜뉴스", "아주경제", "이데일리", "머니투데이", "전자신문", "디지털타임스",
    # 지상파 및 보도/종편 방송사
    "KBS", "MBC", "SBS", "YTN", "연합뉴스TV", "JTBC", "MBN", "채널A", "TV조선", "OBS", "CBS", "BBS",
    # 유력 지역 일간지
    "부산일보", "국제신문", "매일신문", "영남일보", "강원일보", "강원도민일보", "대전일보", "충청투데이", "광주일보", "전남일보", "전북도민일보", "제주일보",
    # 종합 인사 묶음
    "신문", "일보", "방송"
]

def scan_naver_historical_appointments(existing_urls: Set[str] = None) -> List[Dict[str, Any]]:
    print("\n[3/4] 2006~2026 (과거 20년) 네이버 뉴스 전 언론사 정기인사 스캔 시작...")
    articles = []
    seen = set(existing_urls or [])

    years = list(range(2006, 2027))

    for yr in reversed(years):
        year_articles = 0

        # 연말/연초 정기인사 윈도우 (12월 15일 ~ 1월 31일) 및 경제지/주총 후 봄 정기인사 윈도우 (3월 1일 ~ 4월 15일)
        windows = [
            (f"{yr}.12.15", f"{yr+1}.01.31" if yr < 2026 else f"{yr}.12.31"),
            (f"{yr}.03.01", f"{yr}.04.15")
        ]

        for ds_peak, de_peak in windows:
            for outlet in TARGET_OUTLETS:
                query = f'[인사] {outlet}'
                url = f"https://search.naver.com/search.naver?where=news&query={quote(query)}&sort=0&ds={ds_peak}&de={de_peak}&nso=so:r,p:from{ds_peak.replace('.', '')}to{de_peak.replace('.', '')},a:all"
                try:
                    resp = session.get(url, timeout=3.5, verify=False)
                    if resp.status_code == 200:
                        soup = BeautifulSoup(resp.content, "html.parser")
                        tit_elems = soup.select(".news_tit, a.W5fYO2_0xMDeLrlx, a[href*='articleView'], a[href*='article']")
                        for a in tit_elems:
                            h = a.get("href", "")
                            t = a.get_text(strip=True).replace("새 창 열림", "").replace("새창열림", "").strip()
                            if not h or not t or h in seen:
                                continue
                            if not any(k in t for k in ["[인사]", "<인사>", "인사"]):
                                continue
                            seen.add(h)
                            articles.append({
                                "url": h,
                                "title": t,
                                "source": outlet,
                                "date": f"{yr}-12-31"
                            })
                            year_articles += 1
                    time.sleep(0.05)
                except Exception:
                    pass

        print(f"  >> {yr}년도 정기인사 스캔: {year_articles}건 신규 기사 발견 (누적 {len(articles)}건)")

    print(f"  -> 네이버 뉴스 총 {len(articles)}건 발령 기사 확보")
    return articles


# =========================================================================
# 4. 본문 수집, 20년치 전수 파싱 및 듀얼 DB 적재
# =========================================================================
def fetch_body_and_parse(article: Dict[str, Any]) -> List[Dict[str, Any]]:
    url = article.get("url", "")
    title = article.get("title", "")
    given_date = article.get("date", "")
    source = article.get("source", "")

    if not url:
        return []

    try:
        body_res = OnlineNewsTracker.fetch_full_text(url)
        content = body_res.get("text", "")
        if not content or len(content) < 20:
            return []

        pub_date = body_res.get("pub_date") or given_date or datetime.now().strftime("%Y-%m-%d")

        # 언론사명 추정
        target_media = extract_media_from_title(title) or source or "언론사"

        # 파싱 실행
        records = PersonnelRegexParser.parse_article_text(
            article_text=content,
            default_media=target_media,
            article_title=title,
            pub_date=pub_date,
            article_url=url
        )

        valid_records = []
        for r in records:
            med = r.get("media", "")
            dpt = r.get("dept", "")
            if is_media_organization(med) and not is_excluded_department(dpt):
                valid_records.append(r)

        return valid_records
    except Exception:
        return []


def run_20year_batch():
    start_time = time.time()
    print("=" * 70)
    print(" [과거 20년(2006~2026) 전 언론사 정기인사 종합 일괄 수집 및 DB 영구 구축] ")
    print("=" * 70)

    existing_urls = get_existing_urls()
    print(f"기존 DB 수집 완료된 기사 URL: {len(existing_urls)}건 (중복 스킵 처리)")

    all_candidates: List[Dict[str, Any]] = []

    # 1. 한국기자협회보 (1~60페이지)
    c1 = collect_journalist_articles(max_pages=60, existing_urls=existing_urls)
    all_candidates.extend(c1)

    # 2. 중앙일보 피플 (1~60페이지)
    c2 = collect_joongang_articles(max_pages=60, existing_urls=existing_urls)
    all_candidates.extend(c2)

    # 3. 네이버 뉴스 20년 정기인사 스캔 (2006~2026)
    c3 = scan_naver_historical_appointments(existing_urls=existing_urls)
    all_candidates.extend(c3)

    # URL 고유화
    unique_candidates = []
    seen_urls = set(existing_urls)
    for art in all_candidates:
        u = art.get("url", "")
        if u and u not in seen_urls:
            seen_urls.add(u)
            unique_candidates.append(art)

    print("\n" + "=" * 70)
    print(f"신규 수집 대상 고유 발령 기사: 총 {len(unique_candidates)}건")
    print("기사 본문 병렬 수집 및 20년 발령 레코드 전수 파싱 시작 (ThreadPoolExecutor)...")
    print("=" * 70)

    all_parsed_records: List[Dict[str, Any]] = []
    processed_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=14) as executor:
        future_to_art = {executor.submit(fetch_body_and_parse, art): art for art in unique_candidates}
        for future in concurrent.futures.as_completed(future_to_art):
            processed_count += 1
            try:
                recs = future.result()
                if recs:
                    all_parsed_records.extend(recs)
            except Exception:
                pass

            if processed_count % 50 == 0 or processed_count == len(unique_candidates):
                print(f"  기사 본문 파싱 진행: {processed_count}/{len(unique_candidates)} 완료 (누적 발령 레코드 {len(all_parsed_records)}건)")

    print(f"\n[파싱 완료] 20년간 전 언론사에서 추출된 신규 발령 레코드: 총 {len(all_parsed_records)}건")

    if all_parsed_records:
        print("데이터베이스(media_tracker.db)에 신규 레코드 일괄 영구 적재 중...")
        engine = PersonnelTrackerEngine(TRACKER_DB)
        res = engine.process_records(all_parsed_records, append=True)
        print(f"적재 완료: 신규 등록 {res.get('new_records', 0)}건, 누적 고유 언론인 {res.get('unique_reporters', 0)}명, 감지된 이직 {res.get('job_changes_detected', 0)}건")

    # 시드 덤프 갱신
    print("\ndata/archive_seed.json.gz 시드 파일 동기화 덤프 실행...")
    db = MediaDatabase(TRACKER_DB)
    cnt = db.dump_seed_data("data/archive_seed.json.gz")
    print(f"archive_seed.json.gz 동기화 완료: 총 {cnt}건 압축 저장")

    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"20년치 전 언론사 정기인사 일괄 수집 및 DB 영구 구축 작업 완료! (소요 시간: {elapsed:.2f}초)")
    print("=" * 70)


if __name__ == "__main__":
    run_20year_batch()
