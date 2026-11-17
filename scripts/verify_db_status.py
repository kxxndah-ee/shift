import sqlite3
import sys
import os

sys.stdout.reconfigure(encoding="utf-8")

db_path = os.path.join(os.path.dirname(__file__), "..", "media_tracker.db")
conn = sqlite3.connect(db_path)
c = conn.cursor()

c.execute("SELECT SUBSTR(pub_date, 1, 4) as yr, COUNT(*) FROM personnel_records WHERE media LIKE '%헤럴드%' GROUP BY yr ORDER BY yr")
print("=== 헤럴드경제 연도별 현황 ===")
tot = 0
for yr, cnt in c.fetchall():
    print(f"  {yr}년: {cnt}건")
    tot += cnt
print(f"총 {tot}건\n")

c.execute("SELECT COUNT(*) FROM personnel_records")
print(f"총 레코드 수: {c.fetchone()[0]:,}건")
c.execute("SELECT COUNT(*) FROM reporters")
print(f"총 언론인 수: {c.fetchone()[0]:,}명")
c.execute("SELECT COUNT(*) FROM job_changes")
print(f"총 이직/변동 수: {c.fetchone()[0]:,}건\n")

major_outlets = [
    "조선일보", "동아일보", "중앙일보", "한국일보", "경향신문", "한겨레", "국민일보", "서울신문", "세계일보", "문화일보",
    "매일경제", "한국경제", "서울경제", "헤럴드경제", "아시아경제", "파이낸셜뉴스", "머니투데이", "이데일리", "디지털타임스", "전자신문",
    "연합뉴스", "뉴스1", "뉴시스",
    "KBS", "MBC", "SBS", "YTN", "연합뉴스TV", "JTBC", "MBN", "채널A", "TV조선"
]

print("=== 주요 32개 언론사 아카이브 현황 ===")
for outlet in major_outlets:
    c.execute("""
        SELECT SUBSTR(pub_date, 1, 4) as yr, COUNT(*)
        FROM personnel_records
        WHERE media LIKE ?
        GROUP BY yr
        ORDER BY yr
    """, (f"%{outlet}%",))
    rows = c.fetchall()
    cnt_total = sum(r[1] for r in rows)
    years = [r[0] for r in rows]
    min_yr = years[0] if years else "None"
    max_yr = years[-1] if years else "None"
    print(f"{outlet:10s} | {cnt_total:5d}건 | {min_yr} ~ {max_yr} ({len(years):2d}개년)")

non_major_outlets = [
    "부산일보", "국제신문", "매일신문", "영남일보", "대구일보", "경북일보",
    "강원일보", "강원도민일보", "대전일보", "중도일보", "충청투데이", "충청일보",
    "광주일보", "전남일보", "전북도민일보", "전북일보", "경남신문", "경기일보",
    "경인일보", "중부일보", "인천일보", "기호일보", "경상일보",
    "KNN", "TBC", "kbc광주방송", "JTV전주방송", "CJB청주방송", "G1방송", "OBS경인TV",
    "아주경제", "아시아투데이", "이투데이", "뉴스토마토", "비즈워치", "더벨", "딜사이트",
    "데일리안", "프레시안", "오마이뉴스", "기자협회보", "시사저널", "농민신문", "신아일보",
    "브릿지경제", "EBN", "뉴스핌", "뉴스웨이", "스포츠조선", "스포츠서울", "일간스포츠",
    "CBS기독교방송", "BBS불교방송", "CPBC가톨릭평화방송"
]

print("\n=== 기사에서 언급되는 지역/전문/인터넷 언론사 아카이브 현황 ===")
for outlet in non_major_outlets:
    c.execute("""
        SELECT SUBSTR(pub_date, 1, 4) as yr, COUNT(*)
        FROM personnel_records
        WHERE media LIKE ?
        GROUP BY yr
        ORDER BY yr
    """, (f"%{outlet}%",))
    rows = c.fetchall()
    cnt_total = sum(r[1] for r in rows)
    years = [r[0] for r in rows]
    min_yr = years[0] if years else "None"
    max_yr = years[-1] if years else "None"
    print(f"{outlet:12s} | {cnt_total:5d}건 | {min_yr} ~ {max_yr} ({len(years):2d}개년)")
