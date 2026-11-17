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
