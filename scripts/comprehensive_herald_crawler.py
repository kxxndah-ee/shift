import sys
import urllib.parse
import urllib3
import requests
from bs4 import BeautifulSoup
import re
import json
import time

urllib3.disable_warnings()
sys.stdout.reconfigure(encoding='utf-8')

headers_pc = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

all_articles = {}  # url -> {title, url, source, date}

def add_article(url, title, source, date=""):
    if not url or not title:
        return
    url = url.strip()
    if url in all_articles:
        return
    all_articles[url] = {
        "title": title.strip(),
        "url": url,
        "source": source,
        "date": date.strip()
    }

# 1. Daum News Search (2006 ~ 2026)
print("=== 1. Crawling Daum News (2006 ~ 2026) ===")
daum_queries = [
    '[인사] 헤럴드',
    '[인사] 헤럴드경제',
    '[인사] (주)헤럴드',
    '[인사] 헤럴드미디어',
    '[인사] 코리아헤럴드',
    '헤럴드 인사',
    '헤럴드경제 인사',
    '헤럴드 "편집국"',
    '헤럴드 "논설위원"',
    '헤럴드 "마케팅본부"',
    '헤럴드 "디지털콘텐츠국"'
]

for y in range(2006, 2027):
    print(f"  Daum year: {y}...", end=" ", flush=True)
    count_before = len(all_articles)
    for q in daum_queries:
        for page in range(1, 4):
            url = f"https://search.daum.net/search?w=news&nil_search=btn&DA=STC&enc=utf8&cluster=y&cluster_page=1&q={urllib.parse.quote(q)}&sd={y}0101000000&ed={y}1231235959&period=u&p={page}"
            try:
                r = requests.get(url, headers=headers_pc, timeout=4.0, verify=False)
                if r.status_code != 200:
                    break
                soup = BeautifulSoup(r.content, 'html.parser')
                items = soup.select('a.tit_main, a.f_link_b, a[data-tiara-layer="article_title"]')
                if not items:
                    break
                for a in items:
                    t = a.get_text(strip=True)
                    h = a.get('href', '')
                    if any(k in t for k in ['[인사]', '<인사>', '인사', '선임', '승진', '보임', '전보', '발령']):
                        add_article(h, t, "daum", f"{y}-01-01")
            except Exception:
                pass
    print(f"+{len(all_articles) - count_before} articles (total: {len(all_articles)})")

# 2. journalist.or.kr (한국기자협회보)
print("\n=== 2. Crawling journalist.or.kr ===")
journalist_queries = ["헤럴드경제", "헤럴드", "헤럴드미디어", "코리아헤럴드"]
for jq in journalist_queries:
    print(f"  Query: {jq}...", end=" ", flush=True)
    count_before = len(all_articles)
    for p in range(1, 26):
        url = f"https://www.journalist.or.kr/news/search_result.html?search={urllib.parse.quote(jq)}&startdate=&enddate=&sort=1&page={p}"
        try:
            r = requests.get(url, headers=headers_pc, timeout=4.0, verify=False)
            if r.status_code != 200:
                break
            soup = BeautifulSoup(r.content, 'html.parser')
            anchors = soup.find_all('a')
            found_on_page = 0
            for a in anchors:
                h = a.get('href', '')
                t = a.get_text(strip=True)
                if 'article.html?no=' in h and t:
                    if any(k in t for k in ['[인사]', '<인사>', '인사', '선임', '승진', '보임', '전보']):
                        full_url = f"https://www.journalist.or.kr/news/{h}" if not h.startswith('http') else h
                        add_article(full_url, t, "journalist")
                        found_on_page += 1
            if found_on_page == 0 and p > 3:
                break
        except Exception:
            break
    print(f"+{len(all_articles) - count_before} articles")

# 3. Naver News Search (2006 ~ 2026, 2-year windows with pd=3)
print("\n=== 3. Crawling Naver News ===")
naver_queries = [
    '"[인사] 헤럴드"',
    '"[인사]헤럴드"',
    '"[인사] 헤럴드경제"',
    '"[인사]헤럴드경제"',
    '"[인사] (주)헤럴드"',
    '"[인사] (주) 헤럴드"',
    '"[인사] 헤럴드미디어"',
    '"[인사] 코리아헤럴드"',
    '"헤럴드경제" [인사] 연합뉴스',
    '"헤럴드경제" [인사] 뉴스1',
    '"헤럴드경제" [인사] 뉴시스',
    '"헤럴드경제" [인사] 한겨레',
    '"헤럴드경제" [인사] 한국경제',
    '"헤럴드경제" [인사] 매일경제',
    '"헤럴드경제" [인사] 서울경제',
    '"헤럴드" "논설위원실" [인사]',
    '"헤럴드" "편집국" [인사]',
    '"◆헤럴드경제"',
    '"◇헤럴드경제"',
    '"◆헤럴드"',
    '"◇헤럴드"',
    '"[헤럴드경제]"',
    '"[헤럴드]"'
]

intervals = [
    (2006, 2007), (2008, 2009), (2010, 2011), (2012, 2013),
    (2014, 2015), (2016, 2017), (2018, 2019), (2020, 2021),
    (2022, 2023), (2024, 2026)
]

for sy, ey in intervals:
    print(f"  Naver window: {sy}~{ey}...", end=" ", flush=True)
    count_before = len(all_articles)
    for nq in naver_queries:
        for start_idx in [1, 11]:
            url = f"https://search.naver.com/search.naver?where=news&query={urllib.parse.quote(nq)}&sort=0&pd=3&ds={sy}.01.01&de={ey}.12.31&nso=so:r,p:from{sy}0101to{ey}1231,a:all&start={start_idx}"
            try:
                r = requests.get(url, headers=headers_pc, timeout=4.0, verify=False)
                if r.status_code != 200:
                    continue
                soup = BeautifulSoup(r.content, 'html.parser')
                for a in soup.select('a.news_tit, a.W5fYO2_0xMDeLrlx, a[href*="news.naver.com"]'):
                    h = a.get('href', '')
                    t = a.get_text(strip=True).replace('새 창 열림', '').replace('새창열림', '').strip()
                    if not h or not t:
                        continue
                    if any(w in t for w in ['[인사]', '<인사>', '인사', '선임', '승진', '보임', '전보']) and len(t) < 80:
                        add_article(h, t, "naver", f"{sy}-01-01")
            except Exception:
                pass
    print(f"+{len(all_articles) - count_before} articles")

print(f"\n==========================================")
print(f"TOTAL UNIQUE HERALD CANDIDATE ARTICLES: {len(all_articles)}")
print(f"==========================================")

output_path = 'C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/scripts/herald_candidate_articles.json'
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(all_articles, f, ensure_ascii=False, indent=2)
print(f"Saved candidate list to {output_path}")
