import sys
import json
import urllib.parse
import urllib3
import requests
from bs4 import BeautifulSoup
import re
from typing import List, Dict, Any

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

# Update regex patterns in parser dynamically
PersonnelRegexParser.MEDIA_HEADER_PATTERN = re.compile(
    r"(?:◆|◇|■|□|◎|●|⊙|◈|▣|【|\[|<)\s*([가-힣0-9A-Za-z]*(?:일보|경제|뉴스|신문|타임스|타임즈|미디어|데일리|헤럴드|통신|방송|TV|mbc|kbs|sbs|ytn|jtbc|뉴스1|뉴시스|연합뉴스|한겨레|투데이|토마토|블로터|더벨|파이낸셜|팍스넷|지디넷|비즈워치|비즈니스워치|이투데이|뉴스핌))(?:\([^\)]*\))?\s*(?:】|\]|>|◆|◇|■|□|◎|●|⊙|◈|▣)?"
)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

with open('C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/scripts/etoday_candidate_articles.json', encoding='utf-8') as f:
    candidates = json.load(f)

print(f"Loaded {len(candidates)} candidate articles")

parser = PersonnelRegexParser()
all_parsed_records = []
seen_rec_keys = set()

def extract_body_element(soup: BeautifulSoup, url: str) -> str:
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()
        
    for cls_name in ["media_end_linked", "newsct_footer", "byline", "share", "reply", "comment", "copyright", "recommend"]:
        for div in soup.find_all(class_=lambda c: c and cls_name in str(c).lower()):
            div.decompose()

    if "news.naver.com" in url:
        body = soup.find("div", id="newsct_article") or soup.find("div", id="articleBodyContents") or soup.find("article")
        if body:
            return body.get_text()

    if "etoday.co.kr" in url:
        body = (
            soup.find("div", class_=lambda c: c and ("article_body" in c or "view_content" in c or "news_content" in c))
            or soup.find("div", id=lambda i: i and ("article" in i or "viewContent" in i))
        )
        if body:
            return body.get_text()

    if "journalist.or.kr" in url:
        body = soup.find("div", id="article-view-content-div") or soup.find("div", class_="article-body")
        if body:
            return body.get_text()

    if "fnnews.com" in url:
        body = soup.find("div", id="article_content") or soup.find("div", class_="article_body")
        if body:
            return body.get_text()

    if "dt.co.kr" in url:
        body = soup.find("div", id="article_body") or soup.find("div", class_="article_body")
        if body:
            return body.get_text()

    if "segye.com" in url:
        body = soup.find("div", id="article_txt") or soup.find("div", id="article_body")
        if body:
            return body.get_text()

    if "seoul.co.kr" in url:
        body = soup.find("div", id="atic_txt1") or soup.find("div", class_="article_view")
        if body:
            return body.get_text()

    if "hani.co.kr" in url:
        body = soup.find("div", class_="text") or soup.find("div", id="a-left-scroll-in")
        if body:
            return body.get_text()

    main_art = soup.find("article") or soup.find("main")
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
            "기사제보", "독자편집위원회", "윤리강령", "제호 : 이투데이", "뉴스스탠드",
            "댓글", "공감", "추천", "구독자", "AI 음성", "AI 추천", "기자 프로필",
            "동영상 뉴스", "오디오 뉴스", "관련기사", "무단 전재", "본문 폰트 크기"
        ]):
            continue
        line_s = re.sub(r"[\<\[\(]이상\s*[\d월일\s\.\-]+자?[\>\]\)]", "", line_s)
        cleaned_lines.append(line_s)
    return "\n".join(cleaned_lines)

for url, info in candidates.items():
    title = info.get("title", "")
    source = info.get("source", "")
    try:
        resp = requests.get(url, headers=headers, timeout=6, verify=False)
        if resp.status_code != 200:
            continue
        soup = BeautifulSoup(resp.content, "html.parser")
        
        # 1. Date extraction
        pub_date = ""
        if "etoday.co.kr" in url:
            for p in soup.find_all(["p", "span", "div"]):
                t = p.get_text(strip=True)
                if ("입력" in t or "수정" in t) and re.search(r"\d{4}-\d{2}-\d{2}", t):
                    dm = re.search(r"(\d{4}-\d{2}-\d{2})", t)
                    pub_date = dm.group(1)
                    break
        elif "journalist.or.kr" in url:
            byline = soup.find("div", class_="byline")
            if byline:
                dm = re.search(r"(\d{4})[.-](\d{2})[.-](\d{2})", byline.get_text())
                if dm:
                    pub_date = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"
            if not pub_date:
                dm = re.search(r"(\d{4})[.-](\d{2})[.-](\d{2})", soup.get_text())
                if dm:
                    pub_date = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"
        elif "news.naver.com" in url:
            d_span = soup.find("span", class_=lambda c: c and "date" in str(c))
            if d_span:
                dm = re.search(r"(\d{4})[.-](\d{2})[.-](\d{2})", d_span.get_text())
                if dm:
                    pub_date = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"
            if not pub_date:
                dm = re.search(r"(\d{4})[.-](\d{2})[.-](\d{2})", soup.get_text())
                if dm:
                    pub_date = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"
        else:
            dm = re.search(r"(\d{4})[.-](\d{2})[.-](\d{2})", soup.get_text())
            if dm:
                pub_date = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"

        # 2. Body extraction
        raw_body = extract_body_element(soup, url)
        clean_body = clean_article_body_text(raw_body)
        
        # 3. Parse
        default_m = "이투데이" if "etoday.co.kr" in url or "이투데이" in title else ""
        recs = parser.parse_article_text(
            clean_body,
            default_media=default_m,
            article_title=title,
            pub_date=pub_date,
            article_url=url
        )
        
        # 4. Filter only 이투데이 records
        for r in recs:
            norm_m = normalize_media_name(r.get("media", ""))
            if norm_m != "이투데이" and not is_same_media_company(r.get("media", ""), "이투데이"):
                continue
            r["media"] = "이투데이"
            name = parser.clean_name(r.get("name", ""))
            if not name or len(name) < 2 or len(name) > 4:
                continue
            if name in ["보기", "기자", "대표", "부장", "차장", "국장", "실장", "회장", "위원", "이투데이", "단골이", "될까요", "음성", "영상", "서비스", "기자상", "기자연수"]:
                continue
            if is_excluded_department(r.get("dept", "")):
                continue
            
            pdate = r.get("pub_date", "")
            if not pdate or len(pdate) < 10:
                pdate = pub_date or "2010-01-01"
                r["pub_date"] = pdate
                
            dedup_key = (r["pub_date"][:10], r["name"], r.get("dept", ""), r.get("rank_title", ""), r["media"])
            if dedup_key not in seen_rec_keys:
                seen_rec_keys.add(dedup_key)
                all_parsed_records.append(r)
    except Exception as e:
        print(f"Error parsing {url}: {e}")

print(f"\nTotal verified clean 이투데이 records parsed: {len(all_parsed_records)}")
from collections import Counter
years = [r["pub_date"][:4] for r in all_parsed_records]
print("Yearly distribution:")
for yr, cnt in sorted(Counter(years).items()):
    print(f"  {yr}: {cnt}건")

with open('C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/scripts/etoday_parsed_records.json', 'w', encoding='utf-8') as f:
    json.dump(all_parsed_records, f, ensure_ascii=False, indent=2)
print("Saved to scripts/etoday_parsed_records.json")
