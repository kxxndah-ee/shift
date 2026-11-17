import sys
import urllib.parse
import urllib3
import requests
from bs4 import BeautifulSoup
import re

urllib3.disable_warnings()
sys.stdout.reconfigure(encoding='utf-8')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

all_articles = {}  # url -> {title, date, url, source}

# 1. etoday.co.kr
print("=== Crawling etoday.co.kr ===")
etoday_queries = [
    '[인사]이투데이',
    '[인사] 이투데이',
    '이투데이 [인사]',
    '[인사] 이투데이그룹',
    '이투데이 인사',
    '이투데이 "편집국"',
    '이투데이 "승진"',
    '이투데이 "전보"',
    '이투데이 "발령"',
    '이투데이 "선임"',
    '이투데이 "논설위원"',
    '이투데이 "부장"',
    '이투데이 "차장"',
    '이투데이 "국장"',
    '이투데이 "마케팅본부"',
    '이투데이 "경영기획"'
]

for kw in etoday_queries:
    for page in range(1, 15):
        url = f"https://www.etoday.co.kr/search/?keyword={urllib.parse.quote(kw)}&page={page}"
        try:
            resp = requests.get(url, headers=headers, timeout=5, verify=False)
            if resp.status_code != 200:
                break
            soup = BeautifulSoup(resp.content, 'html.parser')
            page_found = 0
            for a in soup.find_all('a'):
                h = a.get('href', '')
                t = a.get_text(strip=True)
                if not h or not t or '/news/view/' not in h or '?trc=' in h:
                    continue
                # 인사 / 발령 / 승진 / 전보 / 보임 / 선임
                if any(w in t for w in ['[인사]', '<인사>', '인사', '선임', '승진', '보임', '전보']) and '이투데이' in t:
                    if h not in all_articles:
                        all_articles[h] = {"title": t, "url": h, "source": "etoday"}
                        page_found += 1
            if page_found == 0 and page > 2:
                break
        except Exception:
            break

print(f"etoday.co.kr unique articles so far: {len(all_articles)}")

# 2. journalist.or.kr
print("\n=== Crawling journalist.or.kr ===")
for p in range(1, 20):
    url = f"https://www.journalist.or.kr/news/search_result.html?search=%EC%9D%B4%ED%88%AC%EB%8D%B0%EC%9D%B4&startdate=&enddate=&sort=1&page={p}"
    try:
        resp = requests.get(url, headers=headers, timeout=5, verify=False)
        soup = BeautifulSoup(resp.content, 'html.parser')
        page_found = 0
        for a in soup.find_all('a'):
            h = a.get('href', '')
            t = a.get_text(strip=True)
            if 'article.html?no=' in h and t:
                if any(k in t for k in ['[인사]', '<인사>', '인사']):
                    full_url = f"https://www.journalist.or.kr/news/{h}" if not h.startswith('http') else h
                    if full_url not in all_articles:
                        all_articles[full_url] = {"title": t, "url": full_url, "source": "journalist"}
                        page_found += 1
        if page_found == 0 and p > 3:
            break
    except Exception:
        break

print(f"Total articles after journalist.or.kr: {len(all_articles)}")

# 3. Naver News targeted search for each 2-year interval from 2006 to 2026
print("\n=== Crawling Naver News ===")
naver_queries = [
    '"[인사] 이투데이"',
    '"[인사]이투데이"',
    '"[인사] 이투데이그룹"',
    '"[인사]이투데이그룹"',
    '"이투데이" [인사] 뉴스1',
    '"이투데이" [인사] 연합뉴스',
    '"이투데이" [인사] 뉴시스',
    '"이투데이" [인사] 한겨레',
    '"이투데이" [인사] 한국일보',
    '"이투데이" [인사] 동아일보',
    '"이투데이" [인사] 매일경제',
    '"이투데이" [인사] 서울경제',
    '"이투데이" [인사] 헤럴드경제',
    '"◆이투데이"',
    '"◇이투데이"',
    '"[이투데이]"'
]

intervals = [
    (2006, 2008), (2009, 2010), (2011, 2012), (2013, 2014),
    (2015, 2016), (2017, 2018), (2019, 2020), (2021, 2022),
    (2023, 2024), (2025, 2026)
]

for sy, ey in intervals:
    for nq in naver_queries:
        for start_idx in [1, 11]:
            url = f"https://search.naver.com/search.naver?where=news&query={urllib.parse.quote(nq)}&sort=0&ds={sy}.01.01&de={ey}.12.31&nso=so:r,p:from{sy}0101to{ey}1231,a:all&start={start_idx}"
            try:
                resp = requests.get(url, headers=headers, timeout=5, verify=False)
                soup = BeautifulSoup(resp.content, 'html.parser')
                for a in soup.select('a.news_tit, a.W5fYO2_0xMDeLrlx'):
                    h = a.get('href', '')
                    t = a.get_text(strip=True).replace('새 창 열림', '').replace('새창열림', '').strip()
                    if not h or not t:
                        continue
                    if any(w in t for w in ['[인사]', '<인사>', '인사', '선임', '승진', '보임', '전보']) and len(t) < 80:
                        if h not in all_articles:
                            all_articles[h] = {"title": t, "url": h, "source": "naver"}
            except Exception:
                pass

print(f"\nTOTAL CANDIDATE ARTICLES COLLECTED: {len(all_articles)}")
import json
with open('C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/scripts/etoday_candidate_articles.json', 'w', encoding='utf-8') as f:
    json.dump(all_articles, f, ensure_ascii=False, indent=2)
print("Saved to scripts/etoday_candidate_articles.json")
