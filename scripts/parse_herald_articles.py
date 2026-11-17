import sys
import json
import urllib.parse
import urllib3
import requests
from bs4 import BeautifulSoup
import re
from typing import List, Dict, Any
from collections import Counter
import concurrent.futures
import threading

urllib3.disable_warnings()
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker')

from data_pipeline import (
    PersonnelRegexParser,
    normalize_media_name,
    is_same_media_company,
    is_media_organization,
    is_department_header,
    is_excluded_department,
    KNOWN_MEDIA_NAMES,
    NON_MEDIA_KEYWORDS
)

# Extend media header patterns to recognize Herald formats
PersonnelRegexParser.MEDIA_HEADER_PATTERN = re.compile(
    r"(?:◆|◇|■|□|◎|●|⊙|◈|▣|【|\[|<)\s*([가-힣0-9A-Za-z]*(?:일보|경제|뉴스|신문|타임스|타임즈|미디어|데일리|헤럴드|통신|방송|TV|mbc|kbs|sbs|ytn|jtbc|뉴스1|뉴시스|연합뉴스|한겨레|투데이|토마토|블로터|더벨|파이낸셜|팍스넷|지디넷|비즈워치|비즈니스워치|이투데이|뉴스핌))(?:\([^\)]*\))?\s*(?:】|\]|>|◆|◇|■|□|◎|●|⊙|◈|▣)?"
)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def extract_body_element(soup: BeautifulSoup, url: str) -> str:
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()
        
    for cls_name in ["media_end_linked", "newsct_footer", "byline", "share", "reply", "comment", "copyright", "recommend", "footer_info", "sns_wrap", "btn_wrap"]:
        for div in soup.find_all(class_=lambda c: c and cls_name in str(c).lower()):
            div.decompose()

    # 1. Daum
    if "daum.net" in url:
        body = soup.select_one("section.article_view, div.article_view, #mArticle, div.alex-target")
        if body:
            return body.get_text()

    # 2. Herald
    if "heraldcorp.com" in url or "heraldbiz.com" in url or "heraldm.com" in url:
        body = (
            soup.find("div", id=lambda i: i and ("articleText" in i or "article_txt" in i or "articleBody" in i or "viewContent" in i))
            or soup.find("div", class_=lambda c: c and ("article_txt" in c or "view_con" in c or "article_view" in c or "article" in c))
        )
        if body:
            return body.get_text()

    # 3. Naver
    if "news.naver.com" in url:
        body = soup.find("div", id="newsct_article") or soup.find("div", id="articleBodyContents") or soup.find("article")
        if body:
            return body.get_text()

    # 4. Journalist
    if "journalist.or.kr" in url:
        body = soup.find("div", id="article-view-content-div") or soup.find("div", class_="article-body")
        if body:
            return body.get_text()

    main_art = soup.find("article") or soup.find("main") or soup.find("div", id="article_body") or soup.find("div", class_="article_body")
    if main_art:
        return main_art.get_text()

    return soup.get_text()

def clean_article_body_text(text: str) -> str:
    cleaned_lines = []
    for line in text.splitlines():
        line_s = line.strip()
        if not line_s:
            continue
        if any(bp in line_s for bp in [
            "기자의 주요 뉴스", "기자의 다른 기사", "전체기사 보기", "한국기자협회의 전체기사",
            "좋아요", "화나요", "슬퍼요", "추가취재", "저작권법", "무단전재", "재배포",
            "기사제보", "독자편집위원회", "윤리강령", "제호 :", "뉴스스탠드",
            "댓글", "공감", "추천", "구독자", "AI 음성", "AI 추천", "기자 프로필",
            "동영상 뉴스", "오디오 뉴스", "관련기사", "무단 전재", "본문 폰트 크기",
            "헤럴드포럼", "헤럴드디자인포럼", "헤럴드경제 기자", "코리아헤럴드 기자"
        ]):
            continue
        line_s = re.sub(r"[\<\[\(]이상\s*[\d월일\s\.\-]+자?[\>\]\)]", "", line_s)
        cleaned_lines.append(line_s)
    return "\n".join(cleaned_lines)

def extract_date(url: str, soup: BeautifulSoup, date_hint: str) -> str:
    # 1. From Daum URL: /v/YYYYMMDD...
    m = re.search(r"/v/(\d{4})(\d{2})(\d{2})", url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # 2. From Herald URL: ud=YYYYMMDD...
    m = re.search(r"ud=(\d{4})(\d{2})(\d{2})", url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # 3. From Daum / Naver / Journalist HTML
    for sel in ["span.num_date", "span.data_info", ".byline", "span.media_end_head_info_datestamp_time"]:
        elem = soup.select_one(sel)
        if elem:
            dm = re.search(r"(\d{4})[.-](\d{2})[.-](\d{2})", elem.get_text())
            if dm:
                return f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"

    # 4. From body text
    dm = re.search(r"(\d{4})[.-](\d{2})[.-](\d{2})", soup.get_text()[:1000])
    if dm:
        return f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"

    # 5. From date hint
    if date_hint and len(date_hint) >= 4:
        return date_hint[:10]

    return "2015-01-01"

def process_single_article(item):
    url, info = item
    title = info.get("title", "")
    date_hint = info.get("date", "")
    
    try:
        resp = requests.get(url, headers=headers, timeout=5, verify=False)
        if resp.status_code != 200:
            return []
        
        # Detect encoding
        if "euc-kr" in resp.text[:1000].lower() or "cp949" in resp.text[:1000].lower():
            resp.encoding = 'euc-kr'
        else:
            resp.encoding = resp.apparent_encoding or 'utf-8'

        soup = BeautifulSoup(resp.text, "html.parser")
        pub_date = extract_date(url, soup, date_hint)
        raw_body = extract_body_element(soup, url)
        clean_body = clean_article_body_text(raw_body)

        default_m = ""
        if any(h in title for h in ["[인사] 헤럴드", "[인사]헤럴드", "[인사] (주)헤럴드", "[인사] 헤럴드경제", "[인사]헤럴드경제"]):
            default_m = "헤럴드경제"
        elif "heraldcorp.com" in url or "heraldbiz.com" in url or "heraldm.com" in url:
            default_m = "헤럴드경제"

        parser = PersonnelRegexParser()
        recs = parser.parse_article_text(
            clean_body,
            default_media=default_m,
            article_title=title,
            pub_date=pub_date,
            article_url=url
        )

        valid_recs = []
        for r in recs:
            raw_m = r.get("media", "").strip()
            norm_m = normalize_media_name(raw_m)
            if not norm_m:
                continue
            if is_same_media_company(raw_m, "헤럴드경제") or "헤럴드" in raw_m or "herald" in raw_m.lower():
                norm_m = "헤럴드경제"
                r["media"] = "헤럴드경제"
            else:
                r["media"] = norm_m

            name = parser.clean_name(r.get("name", ""))
            if not name or len(name) < 2 or len(name) > 4:
                continue
            if name in ["보기", "기자", "대표", "부장", "차장", "국장", "실장", "회장", "위원", "헤럴드", "코리아", "뉴스", "음성", "영상", "서비스", "기자상", "기자연수", "주재", "사무"]:
                continue
            if is_excluded_department(r.get("dept", "")):
                continue

            pdate = r.get("pub_date", "")
            if not pdate or len(pdate) < 10:
                pdate = pub_date
                r["pub_date"] = pdate

            valid_recs.append(r)
        return valid_recs
    except Exception:
        return []

def run_parser():
    cand_path = 'C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/scripts/herald_candidate_articles.json'
    with open(cand_path, encoding='utf-8') as f:
        candidates = json.load(f)

    print(f"Loaded {len(candidates)} candidate articles")

    all_parsed_records = []
    seen_rec_keys = set()
    processed_count = 0

    items = list(candidates.items())
    print(f"Starting parallel parsing with 16 workers...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = {executor.submit(process_single_article, it): it for it in items}
        for f in concurrent.futures.as_completed(futures):
            processed_count += 1
            res_recs = f.result()
            for r in res_recs:
                dedup_key = (r["pub_date"][:10], r["name"], r.get("dept", ""), r.get("rank_title", ""), r["media"])
                if dedup_key not in seen_rec_keys:
                    seen_rec_keys.add(dedup_key)
                    all_parsed_records.append(r)
            if processed_count % 200 == 0 or processed_count == len(items):
                print(f"  Processed {processed_count}/{len(items)} articles, unique records so far: {len(all_parsed_records)}")

    print(f"\nTotal verified clean records parsed: {len(all_parsed_records)}")
    
    herald_records = [r for r in all_parsed_records if r["media"] == "헤럴드경제"]
    print(f"Total 헤럴드경제 records: {len(herald_records)}")
    years = [r["pub_date"][:4] for r in herald_records]
    print("헤럴드경제 Yearly distribution:")
    for yr, cnt in sorted(Counter(years).items()):
        print(f"  {yr}: {cnt}건")

    out_path = 'C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/scripts/herald_parsed_records.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(all_parsed_records, f, ensure_ascii=False, indent=2)
    print(f"Saved to {out_path}")

if __name__ == "__main__":
    run_parser()
