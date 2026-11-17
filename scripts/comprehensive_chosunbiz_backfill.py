import os
import sys
import re
import html
import json
import time
import gzip
import sqlite3
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

all_articles: Dict[str, Dict[str, Any]] = {}

def add_art(url: str, title: str, src: str, year_hint: int = 0):
    if not url or not title:
        return
    u = url.strip()
    if u in all_articles:
        return
    t_clean = title.strip()
    # Exclude non-appointment event articles
    if any(k in t_clean for k in ['포럼', '포토', '사진', '인사말', '인사하고', '인사 나누', '인사말씀', '축사']):
        return
    all_articles[u] = {
        "url": u,
        "title": t_clean,
        "source": src,
        "year_hint": year_hint
    }

print("=== [1단계] 조선비즈 인사 후보 기사 전수 수집 시작 ===")

# 1. 한국기자협회보 (journalist.or.kr)
print("1. 한국기자협회(journalist.or.kr) 조선비즈 수집...")
for page in range(1, 25):
    url = f"https://www.journalist.or.kr/news/search_result.html?search=%EC%A1%B0%EC%84%A0%EB%B9%84%EC%A6%88&sort=1&page={page}"
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
        if found == 0 and page > 4:
            break
    except Exception:
        break
print(f"   -> 기자협회보 완료 (누적: {len(all_articles)}건)")

# 2. Daum News 연도별(2010~2026) 수집
print("2. Daum News 연도별(2010~2026) 조선비즈 수집...")
daum_queries = [
    '[인사] 조선비즈',
    '<인사> 조선비즈',
    '[인사]조선비즈',
    '<인사>조선비즈',
    '조선비즈 [인사]',
    '조선비즈 <인사>',
    '조선비즈 인사',
    '조선비즈 인사 부장',
    '조선비즈 인사 편집국',
    '조선비즈 인사 선임',
    '조선비즈 "대표이사" 선임',
    '조선비즈 "편집국장"',
    '조선비즈 "에디터"',
    '조선비즈 "부장"'
]

for yr in range(2010, 2027):
    for q in daum_queries:
        for p in range(1, 3):
            url = f"https://search.daum.net/search?w=news&nil_search=btn&DA=STC&enc=utf8&cluster=y&cluster_page=1&q={urllib.parse.quote(q)}&sd={yr}0101000000&ed={yr}1231235959&period=u&p={p}"
            try:
                r = requests.get(url, headers=headers, timeout=4, verify=False)
                if r.status_code != 200:
                    continue
                soup = BeautifulSoup(r.content, 'html.parser')
                anchors = soup.find_all('a')
                for a in anchors:
                    h = a.get('href', '')
                    t = a.get_text(strip=True)
                    if ('v.daum.net/v/' in h or 'media.daum.net' in h or 'news.v.daum.net' in h) and t:
                        if any(k in t for k in ['[인사]', '<인사>', '인사', '선임', '승진', '보임', '전보']):
                            add_art(h, t, "daum", yr)
            except Exception:
                pass
print(f"   -> Daum 완료 (누적: {len(all_articles)}건)")

print(f"\n=======================================================")
print(f"총 수집된 조선비즈 인사 후보 기사: {len(all_articles)}건")
print(f"=======================================================")

# [2단계] 기사 본문 추출 및 정밀 파싱
print("\n=== [2단계] 기사 본문 수집 및 정밀 일자/발령 파싱 ===")

def extract_exact_date(url: str, soup: BeautifulSoup, yr_hint: int) -> str:
    m = re.search(r"/v/(\d{4})(\d{2})(\d{2})", url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"[/=](\d{4})(\d{2})(\d{2})", url)
    if m and 2000 <= int(m.group(1)) <= 2026:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    for meta_prop in ["article:published_time", "og:regDate", "pubdate", "sailthru.date", "date"]:
        meta = soup.select_one(f"meta[property='{meta_prop}'], meta[name='{meta_prop}']")
        if meta and meta.get("content"):
            dm = re.search(r"(20[1-2][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", meta["content"])
            if dm:
                return f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"

    for sel in [
        ".media_end_head_info_datestamp_time", "._ARTICLE_DATE_TIME",
        "span.num_date", "span.data_info", "span.date", ".dates",
        ".byline", ".article_info", ".txt_info", "div.date_area", ".date_wrap", ".news_date"
    ]:
        elem = soup.select_one(sel)
        if elem:
            dm = re.search(r"(20[1-2][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", elem.get_text())
            if dm:
                return f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"

    dm = re.search(r"(20[1-2][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})[일자]?", soup.get_text()[:1000])
    if dm:
        return f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"

    if yr_hint and 2010 <= yr_hint <= 2026:
        return f"{yr_hint}-01-01"

    return ""

def extract_clean_article_body(soup: BeautifulSoup, url: str) -> str:
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()

    stop_markers = [
        '※ 저작권자', 'Copyright', 'ⓒ', '무단 전재', '무단전재',
        '저작권자 ⓒ', '무단 전재-재배포', '무단전재 및 재배포',
        '파이낸셜뉴스에서 직접 확인하세요', '해당 언론사로 이동합니다',
        '기자협회보 무단전재'
    ]

    # Daum news
    if "daum.net" in url:
        p_tags = soup.select("div[dmcf-ptype='general'], section.article_view p, div.article_view p")
        if p_tags:
            paragraphs = []
            for p in p_tags:
                t = p.get_text().strip()
                if not t:
                    continue
                if any(m in t for m in stop_markers):
                    break
                paragraphs.append(t)
            if paragraphs:
                return "\n".join(paragraphs)

    # journalist.or.kr
    if "journalist.or.kr" in url:
        b = soup.find("div", id="article-view-content-div") or soup.find("div", class_="article-body")
        if b:
            paragraphs = []
            for line in b.get_text(separator="\n").split("\n"):
                t = line.strip()
                if not t:
                    continue
                if any(m in t for m in stop_markers):
                    break
                paragraphs.append(t)
            if paragraphs:
                return "\n".join(paragraphs)

    # Fallback with line-by-line stop marker check
    b = soup.select_one("section.article_view, div.article_view, #mArticle, #newsct_article")
    raw_lines = (b or soup).get_text(separator="\n").split("\n")
    paragraphs = []
    for line in raw_lines:
        t = line.strip()
        if not t:
            continue
        if any(h in t for h in ['음성으로 듣기', '음성재생 설정', '글씨크기', '번역 설정', '인쇄하기']):
            continue
        if any(m in t for m in stop_markers):
            break
        paragraphs.append(t)
    return "\n".join(paragraphs)

HANGUL_NAME_RE = re.compile(r'^[가-힣]{2,4}$')
NOISE_NAME_WORDS = {
    '기자', '기자상', '기자연', '기자연수', '보기', '보는', '모친은', '찾아', '종합', '사진', '뉴스', '배포',
    '전재', '등등', '사무', '직무', '대리', '위원', '본부', '부장', '팀장', '국장', '실장',
    '총괄', '논설', '전문', '대표', '상무', '전무', '이사', '취재', '편집', '보도', '미디어',
    '발견', '실종', '숨진', '행보', '사태', '경찰', '검찰', '다운받기', '바로가기', '포토',
    '카드뉴스', '동영상', '구독', '채널', '제보', '문의', '사과', '안내', '공지', '아들', '차에', '치여', '숨져',
    '전기요금', '넣은', '모바일앱', '진술거부', '가해자', '피해자', '부모', '어머니', '아버지',
    '손해배상', '사전예약', '완판', '상업화', '속도', '한국과', '수출', '격차', '커지나', '리스크',
    '소통능력', '신화', '주역', '사령탑', '오른', '경영', '분리', '부진', '돌파구', '마련', '특명',
    '비중', '늘리면', '전력', '생산단가', '인하', '가능', '꺾인', '실적은', '부담', '부탄에', '기후공원',
    '세우다', '결실', '코이카', '가동', '참수', '영상', '유흥업소', '부모님께', '부끄럽다', '탈의실', '천장에서',
    '상간남', '전체기사', '기자', '기자들', '고소', '유포자', '사랑꾼', '이미지', '타격', '피소',
    '한국어', '중국어', '일본어', '영어', '태국어', '독일어', '프랑스어', '스페인어'
}

NOISE_DEPT_WORDS = [
    '모바일', '연합뉴스', '발견', '실종', '숨진', '채널', '저작권', '무단전재', '재배포', 'http',
    '살던', '살해', '체포', '7070', 'NATE', '연료비', '양파망', '공분', '횡단보도', '검거',
    '둔기', '피의자', '구속', '경찰', '소방', '순천만', '국제정원박람회', '송광면장', '다산2동장',
    '진건읍', '도로시설관리', '도시개발', '소년대원', '흉기', '참수', '루머', '징역', '뇌종양',
    '와르르', '탈의실', '천장', '피소', '사랑꾼', 'MK픽', '상간남', '박수환', '뉴스타파',
    '전체기사', '문자', '의혹', '…', '...', '출범 이후 경제계', '인터뷰'
]

def slice_chosunbiz_section(body: str, title: str) -> str:
    # If the title is strictly about ChosunBiz (e.g. '[인사] 조선비즈')
    clean_t = title.replace(" ", "")
    if ("조선비즈" in clean_t and any(k in clean_t for k in ['[인사]', '<인사>']) and
        not any(k in clean_t for k in ['외', '外', '·', ',', '등'])):
        return body

    lines = body.split("\n")
    in_chosunbiz = False
    chosunbiz_lines = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Look for section headers: e.g. ◆조선비즈, ◇조선비즈, ▲조선비즈, ⊙조선비즈, [조선비즈]
        header_match = re.search(r'^[▲◇◆[◎○●□■▷▶◈⊙]\s*([가-힣\w\s/·]+)', stripped)
        if header_match:
            hdr_text = header_match.group(1).strip()
            if "조선비즈" in hdr_text:
                in_chosunbiz = True
                chosunbiz_lines.append(stripped)
                continue
            elif in_chosunbiz:
                break
        elif in_chosunbiz:
            # Check if an un-bulleted new organization header appears
            if re.match(r'^[가-힣A-Za-z0-9\s]{2,15}\s*$', stripped) and not any(r in stripped for r in ['부장', '팀장', '국장', '실장', '본부장', '에디터', '대표', '센터장']):
                break
            chosunbiz_lines.append(stripped)

    if chosunbiz_lines:
        return "\n".join(chosunbiz_lines)

    if "조선비즈" in clean_t:
        return body

    return ""

extracted_records: List[Dict[str, Any]] = []

def process_art(art_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    u = art_info["url"]
    t = art_info["title"]
    yr_hint = art_info.get("year_hint", 0)
    try:
        r = requests.get(u, headers=headers, timeout=5, verify=False)
        if r.status_code != 200:
            return []
        if "euc-kr" in r.text[:1000].lower() or "cp949" in r.text[:1000].lower():
            r.encoding = 'euc-kr'
        else:
            r.encoding = r.apparent_encoding or 'utf-8'

        soup = BeautifulSoup(r.text, "html.parser")
        pub_d = extract_exact_date(u, soup, yr_hint)
        if not pub_d or pub_d.startswith("2026-11-17"):  # 허위 오늘 날짜 차단
            return []

        body = extract_clean_article_body(soup, u)
        if not body or len(body) < 10:
            return []

        target_text = slice_chosunbiz_section(body, t)
        if not target_text:
            return []

        recs = PersonnelRegexParser.parse_article_text(
            article_text=target_text,
            default_media="조선비즈",
            article_title=t,
            pub_date=pub_d,
            article_url=u
        )

        valid_recs = []
        for rec in recs:
            med = normalize_media_name(rec.get("media", ""))
            if "조선비즈" in med or med == "조선비즈":
                rec["media"] = "조선비즈"
                name = rec.get("name", "").strip()
                dept = rec.get("dept", "").strip()
                rank_t = rec.get("rank_title", "").strip()

                if not HANGUL_NAME_RE.match(name) or name in NOISE_NAME_WORDS:
                    continue
                if len(dept) > 20 or any(k in dept for k in NOISE_DEPT_WORDS):
                    continue
                if len(rank_t) > 15 or any(k in rank_t for k in ['모바일', '7070', 'NATE', '연합뉴스']):
                    continue
                if is_excluded_department(dept):
                    continue

                valid_recs.append(rec)
        return valid_recs
    except Exception:
        return []

print(f"총 {len(all_articles)}개 기사 본문 병렬 다운로드 및 정밀 파싱 실행 (16 워커)...")
with ThreadPoolExecutor(max_workers=16) as pool:
    futures = [pool.submit(process_art, a) for a in all_articles.values()]
    done_cnt = 0
    for fut in as_completed(futures):
        res = fut.result()
        if res:
            extracted_records.extend(res)
        done_cnt += 1
        if done_cnt % 50 == 0 or done_cnt == len(all_articles):
            print(f"  [{done_cnt}/{len(all_articles)}] 처리 완료... 현재 유효 추출 레코드: {len(extracted_records)}건")

print(f"\n=======================================================")
print(f"총 추출된 유효 조선비즈 발령 레코드: {len(extracted_records)}건")
print(f"=======================================================")

# [3단계] 기존 조선비즈 완전 정제 및 데이터베이스 병합
print("\n=== [3단계] 데이터베이스 완전 정제 및 신규 백필 병합 ===")
db_path = os.path.join(PROJECT_ROOT, "media_tracker.db")
conn = sqlite3.connect(db_path)
c = conn.cursor()

# 기존 조선비즈 레코드 중 명백한 노이즈 정리
c.execute("SELECT id, dept, rank_title, name, article_title FROM personnel_records WHERE media = '조선비즈'")
existing_rows = c.fetchall()
del_ids = []
for rid, dept, rank_t, name, title in existing_rows:
    if not HANGUL_NAME_RE.match(name) or name in NOISE_NAME_WORDS:
        del_ids.append(rid)
    elif len(dept) > 20 or any(k in dept for k in NOISE_DEPT_WORDS):
        del_ids.append(rid)
    elif title and any(k in title for k in ['거물급 인사 인터뷰', '박수환 문자', '뉴스타파']):
        del_ids.append(rid)

if del_ids:
    print(f"기존 DB 내 조선비즈 노이즈 레코드 {len(del_ids)}건 삭제 중...")
    c.executemany("DELETE FROM personnel_records WHERE id = ?", [(rid,) for rid in del_ids])
    conn.commit()

conn.close()

# 신규 추출 레코드 안전 병합
engine = PersonnelTrackerEngine()
db = MediaDatabase()

res_summary = engine.process_records(extracted_records, append=True)
print(f"DB 병합 결과: 언론인 마스터 {res_summary['unique_reporters']}명, 이직 감지 {res_summary['job_changes_detected']}건")

# 마스터 시드 완전 직렬화 갱신
with db.get_connection() as conn:
    c = conn.cursor()
    c.execute("SELECT * FROM personnel_records")
    all_recs = [dict(r) for r in c.fetchall()]
    c.execute("SELECT * FROM reporters")
    all_reps = [dict(r) for r in c.fetchall()]
    c.execute("SELECT * FROM job_changes")
    all_jc = [dict(r) for r in c.fetchall()]

payload = {
    "personnel_records": all_recs,
    "reporters": all_reps,
    "job_changes": all_jc
}

seed_path = os.path.join(PROJECT_ROOT, "data", "archive_seed.json.gz")
with gzip.open(seed_path, "wt", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False)

print(f"마스터 시드 갱신 완료: 총 {len(all_recs):,}건 저장 ({seed_path})")

# [4단계] 최종 조선비즈 연도별 커버리지 확인
print("\n=== [4단계] 조선비즈 백필 결과 검증 ===")
with db.get_connection() as conn:
    c = conn.cursor()
    c.execute("""
        SELECT substr(pub_date, 1, 4) as yr, count(*)
        FROM personnel_records
        WHERE media = '조선비즈'
        GROUP BY yr
        ORDER BY yr
    """)
    rows = c.fetchall()
    print("조선비즈 최종 연도별 정제 레코드:")
    total = 0
    for yr, cnt in rows:
        print(f"  {yr}년: {cnt}건")
        total += cnt
    print(f"총 조선비즈 발령 레코드: {total}건 (최초 시작 연도: {rows[0][0] if rows else '없음'}년)")
