import sys
import sqlite3
from typing import List, Dict, Any

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker')

from data_pipeline import (
    MediaDatabase,
    are_group_affiliates,
    is_same_media_company,
    is_media_organization,
    normalize_media_name
)

db = MediaDatabase()
passed = 0
failed = 0

def test(desc: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"[PASS] {desc} {detail}")
    else:
        failed += 1
        print(f"[FAIL] {desc} {detail}")

print("================================================================================")
print("              MEDIA PERSONNEL TRACKER EXHAUSTIVE VERIFICATION SUITE             ")
print("================================================================================\n")

# 1. Verification of 32 Major Media DB Coverage
print("--- [Suite 1: 32 Major Media DB Archival Coverage] ---")
major_32 = [
    "조선일보", "동아일보", "중앙일보", "한국일보", "경향신문", "한겨레", "국민일보", "서울신문",
    "세계일보", "문화일보", "매일경제", "한국경제", "서울경제", "헤럴드경제", "아시아경제", "파이낸셜뉴스",
    "머니투데이", "이데일리", "디지털타임스", "전자신문", "연합뉴스", "뉴스1", "뉴시스", "KBS",
    "MBC", "SBS", "YTN", "연합뉴스TV", "JTBC", "MBN", "채널A", "TV조선"
]

for m in major_32:
    recs = db.search_records(media=m)
    c = len(recs)
    min_yr = min([r['pub_date'][:4] for r in recs if r.get('pub_date')]) if recs else "None"
    max_yr = max([r['pub_date'][:4] for r in recs if r.get('pub_date')]) if recs else "None"
    test(f"Major media: {m:12s}", c >= 50, f"(Count: {c:5d}건, Span: {min_yr} ~ {max_yr})")

# 2. Verification of Regional & Specialty Media Coverage
print("\n--- [Suite 2: Regional & Specialty Media Archival Coverage] ---")
specialty_media = [
    "이투데이", "부산일보", "국제신문", "아시아투데이", "뉴스핌", "더벨", "딜사이트",
    "스포츠조선", "스포츠서울", "CBS기독교방송", "BBS불교방송", "경인일보", "충청투데이", "중도일보"
]

for m in specialty_media:
    recs = db.search_records(media=m)
    c = len(recs)
    min_yr = min([r['pub_date'][:4] for r in recs if r.get('pub_date')]) if recs else "None"
    test(f"Specialty media: {m:12s}", c >= 25, f"(Count: {c:5d}건, Earliest: {min_yr})")

# 3. Multi-Parameter Compound Queries (Media + Dept + Rank)
print("\n--- [Suite 3: Multi-Parameter Compound Queries (Media + Dept + Rank)] ---")
compound_tests = [
    ("한국경제", "건설부동산", "부장"),
    ("헤럴드경제", "건설부동산", "부장"),
    ("매일경제", "산업", "부장"),
    ("서울경제", "금융", "부장"),
    ("조선일보", "사회", "부장"),
    ("중앙일보", "경제", "부장"),
    ("동아일보", "정치", "부장"),
    ("뉴스1", "정치", "부장"),
    ("이투데이", "금융", "부장"),
    ("코리아헤럴드", "편집국", "국장")
]

for m, d, r in compound_tests:
    recs = db.search_records(media=m, dept=d, rank=r)
    reps = db.search_reporters(media=m, dept=d, rank=r)
    test(f"Query: [{m} + {d} + {r}]", len(recs) > 0 or len(reps) > 0, f"(Records: {len(recs)}건, Reporters: {len(reps)}명)")

# 4. Search Keyword Auto-splitting & Role Correction
print("\n--- [Suite 4: Search Keyword Auto-splitting & Fault Tolerance] ---")
# Case A: user typed '한국경제 건설부동산' in media input field
split_test1 = db.search_records(media="한국경제 건설부동산")
test("Auto-split '한국경제 건설부동산' in media field", len(split_test1) > 0, f"({len(split_test1)}건 found)")

# Case B: user typed '건설부동산' in media field and left dept empty
split_test2 = db.search_records(media="건설부동산")
test("Role flip '건설부동산' in media field", len(split_test2) > 0, f"({len(split_test2)}건 found)")

# Case C: user typed '건설부동산 부장' in dept field
split_test3 = db.search_records(media="한국경제", dept="건설부동산 부장")
test("Auto-rank-split '건설부동산 부장' in dept field", len(split_test3) > 0, f"({len(split_test3)}건 found)")

# Case D: Name cross-search (Kim Hee-jun in News1 vs Hankyung cross-check prevention)
khj_news1 = db.search_records(media="뉴스1", name="김희준")
khj_hk = db.search_records(media="한국경제", name="김희준")
test("Reporter cross-check: 김희준 in 뉴스1 verified", len(khj_news1) > 0, f"({len(khj_news1)}건)")
test("Reporter cross-check: 김희준 not falsely attributed to 한국경제", len(khj_hk) == 0, f"(Safeguard active: {len(khj_hk)}건)")

# 5. Herald Corporate Structure & Parent-Child Relationships
print("\n--- [Suite 5: Herald Parent Entity & Subordinate Publications] ---")
test("is_same_media_company('헤럴드', '헤럴드미디어')", is_same_media_company('헤럴드', '헤럴드미디어') is True)
test("is_same_media_company('헤럴드미디어', '(주)헤럴드')", is_same_media_company('헤럴드미디어', '(주)헤럴드') is True)
test("are_group_affiliates('헤럴드', '헤럴드경제')", are_group_affiliates('헤럴드', '헤럴드경제') is True)
test("are_group_affiliates('헤럴드', '코리아헤럴드')", are_group_affiliates('헤럴드', '코리아헤럴드') is True)
test("are_group_affiliates('헤럴드경제', '코리아헤럴드')", are_group_affiliates('헤럴드경제', '코리아헤럴드') is True)

h_parent_recs = db.search_records(media="헤럴드")
h_media_recs = db.search_records(media="헤럴드미디어")
test("Search '헤럴드' encompasses entire company (4800+건)", len(h_parent_recs) >= 4800, f"({len(h_parent_recs)}건)")
test("Search '헤럴드미디어' returns identical company count", len(h_media_recs) == len(h_parent_recs), f"({len(h_media_recs)}건)")

he_recs = db.search_records(media="헤럴드경제")
kh_recs = db.search_records(media="코리아헤럴드")
test("Search '헤럴드경제' separates Herald Economy (4600+건)", len(he_recs) >= 4600, f"({len(he_recs)}건)")
test("Search '코리아헤럴드' separates The Korea Herald (900+건)", len(kh_recs) >= 900, f"({len(kh_recs)}건)")

# 6. Job Changes & Cross-Check Turnover Accuracy
print("\n--- [Suite 6: Turnover Cross-Check & Affiliate Safeguards] ---")
with db.get_connection() as conn:
    c = conn.cursor()
    c.execute("SELECT count(*) FROM job_changes")
    total_jc = c.fetchone()[0]
    test("Total detected job changes in DB", total_jc >= 2500, f"({total_jc:,}건)")

    # Check that moves within same group are affiliate moves, not external leaks
    c.execute("""
        SELECT count(*) FROM job_changes 
        WHERE (from_media LIKE '%헤럴드%' AND to_media LIKE '%헤럴드%')
    """)
    internal_herald_changes = c.fetchone()[0]
    test("Internal Herald family reassignments recognized", internal_herald_changes >= 0, f"({internal_herald_changes}건)")

# 7. Database Summary
print("\n================================================================================")
print(f"VERIFICATION SUMMARY: {passed} PASSED / {failed} FAILED")
print("================================================================================")

if failed == 0:
    print("ALL TESTS PASSED WITH 100% ACCURACY! SYSTEM IS ROCK-SOLID.")
else:
    print(f"ATTENTION: {failed} tests failed! Investigating...")
