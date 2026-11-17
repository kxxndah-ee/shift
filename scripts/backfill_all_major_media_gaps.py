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
    normalize_media_name
)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
}

# 각 매체별 누락 연도 맵
TARGET_MEDIA_GAPS: Dict[str, List[int]] = {
    "조선일보": [2007, 2009, 2010, 2011, 2014, 2015, 2016, 2017],
    "동아일보": [2007, 2009, 2010, 2016, 2019, 2020],
    "한국일보": [2019],
    "경향신문": [2007, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2020],
    "한겨레": [2009, 2013, 2015, 2017, 2020],
    "국민일보": [2017, 2020, 2022],
    "서울신문": [2009, 2010, 2012, 2015, 2016, 2021],
    "세계일보": [2007, 2009, 2014, 2015, 2016, 2017],
    "문화일보": [2006, 2007, 2008, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2019, 2020, 2021, 2023, 2025],
    "서울경제": [2006, 2007, 2009, 2012, 2019],
    "아시아경제": [2008, 2009, 2011, 2012, 2014, 2015, 2016, 2017, 2019, 2020, 2021],
    "파이낸셜뉴스": [2006, 2007, 2010, 2014, 2015, 2016, 2021],
    "머니투데이": [2006, 2009, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
    "디지털타임스": [2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2015, 2016, 2017, 2020, 2021, 2022, 2023, 2024],
    "전자신문": [2006, 2007, 2008, 2009, 2011, 2013, 2015, 2016, 2017, 2020, 2021, 2023, 2024],
    "뉴시스": [2006, 2007, 2008, 2009, 2010, 2011, 2012, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2025],
    "뉴스핌": [2006, 2007, 2008, 2009, 2010, 2011, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2025],
    "더벨": [2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2019, 2020, 2021, 2022, 2023, 2024],
    "딜사이트": [2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2024, 2025, 2026],
    "MBC": [2006, 2007, 2009, 2012, 2013, 2014, 2015, 2016, 2017, 2020, 2021, 2023],
    "SBS": [2006, 2013, 2014, 2015, 2016, 2017, 2021],
    "YTN": [2007, 2009, 2010, 2011, 2012, 2013, 2014, 2016, 2017, 2019, 2020, 2023, 2026],
    "연합뉴스TV": [2006, 2007, 2008, 2009, 2010, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021],
    "JTBC": [2006, 2007, 2008, 2009, 2010, 2011, 2013, 2014, 2015, 2016, 2017],
    "MBN": [2006, 2007, 2008, 2009, 2010, 2013, 2014, 2015, 2016, 2017, 2019, 2020, 2024],
    "채널A": [2006, 2007, 2008, 2009, 2010, 2012, 2013, 2015, 2016, 2017, 2019, 2020, 2023],
    "TV조선": [2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2019, 2020, 2021, 2022, 2025]
}

all_articles: Dict[str, Dict[str, Any]] = {}

def add_art(url: str, title: str, src: str, target_media: str, date_hint: str = ""):
    if not url or not title:
        return
    u = url.strip()
    if u in all_articles:
        return
    all_articles[u] = {
        "url": u,
        "title": title.strip(),
        "source": src,
        "target_media": target_media,
        "date_hint": date_hint.strip()
    }

print("=== [1단계] 주요 매체 누락 연도 기사 전수 수집 시작 ===")

# 1. 한국기자협회보 (journalist.or.kr) 수집 (누락 매체별 1~12페이지)
print("1. 한국기자협회(journalist.or.kr) 매체별 수집...")
for med in TARGET_MEDIA_GAPS.keys():
    for page in range(1, 11):
        url = f"https://www.journalist.or.kr/news/search_result.html?search={urllib.parse.quote(med)}&sort=1&page={page}"
        try:
            r = requests.get(url, headers=headers, timeout=4, verify=False)
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
                        add_art(full_u, t, "journalist", med)
                        found += 1
            if found == 0 and page > 2:
                break
        except Exception:
            break
print(f"   -> 기자협회보 완료 (누적: {len(all_articles)}건)")

# 2. Daum News 누락 연도 타겟 수집
print("2. Daum News 누락 연도 타겟 수집...")
for med, gap_years in TARGET_MEDIA_GAPS.items():
    for yr in gap_years:
        for q in [f'[인사] {med}', f'{med} 인사']:
            url = f"https://search.daum.net/search?w=news&nil_search=btn&DA=STC&enc=utf8&cluster=y&cluster_page=1&q={urllib.parse.quote(q)}&sd={yr}0101000000&ed={yr}1231235959&period=u&p=1"
            try:
                r = requests.get(url, headers=headers, timeout=3.5, verify=False)
                if r.status_code != 200:
                    continue
                soup = BeautifulSoup(r.content, 'html.parser')
                items = soup.select('a.tit_main, a.f_link_b, a[data-tiara-layer="article_title"]')
                for a in items:
                    t = a.get_text(strip=True)
                    h = a.get('href', '')
                    if any(k in t for k in ['[인사]', '<인사>', '인사', '선임', '승진', '보임', '전보']) and med in t:
                        add_art(h, t, "daum", med, f"{yr}-01-01")
            except Exception:
                pass
print(f"   -> Daum 완료 (누적: {len(all_articles)}건)")

print(f"\n=======================================================")
print(f"총 수집된 주요 매체 결측치 후보 기사: {len(all_articles)}건")
print(f"=======================================================")

# [2단계] 기사 본문 추출 및 정밀 날짜 파싱
print("\n=== [2단계] 기사 본문 수집 및 정밀 일자/발령 파싱 ===")

def extract_exact_date(url: str, soup: BeautifulSoup, date_hint: str) -> str:
    m = re.search(r"/v/(\d{4})(\d{2})(\d{2})", url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"[/=](\d{4})(\d{2})(\d{2})", url)
    if m and 2000 <= int(m.group(1)) <= 2026:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    for meta_prop in ["article:published_time", "og:regDate", "pubdate", "sailthru.date", "date"]:
        meta = soup.select_one(f"meta[property='{meta_prop}'], meta[name='{meta_prop}']")
        if meta and meta.get("content"):
            dm = re.search(r"(20[0-2][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", meta["content"])
            if dm:
                return f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"

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

    dm = re.search(r"(20[0-2][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})[일자]?", soup.get_text()[:1000])
    if dm:
        return f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"

    if date_hint and len(date_hint) >= 4:
        return date_hint[:10]

    return ""

def extract_body(soup: BeautifulSoup, url: str) -> str:
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()

    for cls_name in ["media_end_linked", "newsct_footer", "byline", "share", "reply", "comment", "copyright", "recommend", "footer_info", "sns_wrap", "btn_wrap"]:
        for div in soup.find_all(class_=lambda c: c and cls_name in str(c).lower()):
            div.decompose()

    if "daum.net" in url:
        b = soup.select_one("section.article_view, div.article_view, #mArticle, div.alex-target")
        if b:
            return b.get_text(separator="\n")

    if "news.naver.com" in url:
        b = soup.find("div", id="newsct_article") or soup.find("div", id="articleBodyContents") or soup.find("article")
        if b:
            return b.get_text(separator="\n")

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
    med_target = art_info["target_media"]
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
            default_media=med_target,
            article_title=t,
            pub_date=pub_d,
            article_url=u
        )

        valid_recs = []
        for rec in recs:
            rec_med = normalize_media_name(rec.get("media", ""))
            # 실제 대상 언론사 및 언론 기관 발령만 등록
            if is_media_organization(rec_med) and not is_excluded_department(rec.get("dept", "")) and rec.get("name") and len(rec["name"]) >= 2:
                rec["media"] = rec_med
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
        if done_cnt % 100 == 0 or done_cnt == len(all_articles):
            print(f"  [{done_cnt}/{len(all_articles)}] 처리 완료... 현재 추출 레코드: {len(extracted_records)}건")

print(f"\n=======================================================")
print(f"총 추출된 유효 주요 매체 발령 레코드: {len(extracted_records)}건")
print(f"=======================================================")

# [3단계] DB 및 archive_seed.json.gz에 병합 영속화
print("\n=== [3단계] 데이터베이스 및 마스터 시드 파일에 안전 병합 ===")
engine = PersonnelTrackerEngine()
db = MediaDatabase()

res_summary = engine.process_records(extracted_records, append=True)
print(f"DB 병합 결과: 언론인 마스터 {res_summary['unique_reporters']}명, 이직 감지 {res_summary['job_changes_detected']}건")

all_db_records = db.get_all_raw_records()
import gzip
seed_path = os.path.join(PROJECT_ROOT, "data", "archive_seed.json.gz")
with gzip.open(seed_path, "wt", encoding="utf-8") as f:
    json.dump([dict(r) for r in all_db_records], f, ensure_ascii=False, indent=2)

print(f"마스터 시드 갱신 완료: 총 {len(all_db_records):,}건 저장 ({seed_path})")
