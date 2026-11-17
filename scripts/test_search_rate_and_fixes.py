import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_pipeline import MediaDatabase, parse_composite_search_query

def test_composite_parsing():
    print("--- Testing parse_composite_search_query ---")
    t1 = parse_composite_search_query("한국경제 건설부동산 부장")
    assert t1["media"] == "한국경제", f"Expected 한국경제, got {t1['media']}"
    assert t1["dept"] == "건설부동산", f"Expected 건설부동산, got {t1['dept']}"
    assert t1["rank"] == "부장", f"Expected 부장, got {t1['rank']}"
    assert t1["name"] == "", f"Expected empty name, got {t1['name']}"
    print("[PASS] '한국경제 건설부동산 부장' parsed correctly:", t1)

    t2 = parse_composite_search_query("헤럴드경제 김동호")
    assert t2["media"] == "헤럴드경제", f"Expected 헤럴드경제, got {t2['media']}"
    assert t2["name"] == "김동호", f"Expected 김동호, got {t2['name']}"
    print("[PASS] '헤럴드경제 김동호' parsed correctly:", t2)

    t3 = parse_composite_search_query("건설부동산 부장")
    assert t3["dept"] == "건설부동산", f"Expected 건설부동산, got {t3['dept']}"
    assert t3["rank"] == "부장", f"Expected 부장, got {t3['rank']}"
    print("[PASS] '건설부동산 부장' parsed correctly:", t3)

    t4 = parse_composite_search_query("김동호")
    assert t4["name"] == "김동호", f"Expected 김동호, got {t4['name']}"
    print("[PASS] '김동호' parsed correctly:", t4)

def test_database_queries():
    print("\n--- Testing Database Queries for Search Scenarios ---")
    db = MediaDatabase("media_tracker.db")
    
    # 1. 한국경제 건설부동산 부장
    recs = db.search_records(media="한국경제", dept="건설부동산", rank="부장")
    assert len(recs) == 13, f"Expected 13 records, got {len(recs)}"
    print(f"[PASS] 한국경제 건설부동산 부장 query: {len(recs)} records found")

    # 2. 헤럴드경제 건설부동산 부장
    recs_herald = db.search_records(media="헤럴드경제", dept="건설부동산", rank="부장")
    assert len(recs_herald) >= 10, f"Expected >= 10 records, got {len(recs_herald)}"
    print(f"[PASS] 헤럴드경제 건설부동산 부장 query: {len(recs_herald)} records found")

    # 3. 서울경제
    recs_seoul = db.search_records(media="서울경제")
    assert len(recs_seoul) >= 400, f"Expected >= 400 records, got {len(recs_seoul)}"
    print(f"[PASS] 서울경제 query: {len(recs_seoul)} records found")

    # 4. 이투데이
    recs_etoday = db.search_records(media="이투데이")
    assert len(recs_etoday) >= 400, f"Expected >= 400 records, got {len(recs_etoday)}"
    print(f"[PASS] 이투데이 query: {len(recs_etoday)} records found")

if __name__ == "__main__":
    test_composite_parsing()
    test_database_queries()
    print("\nAll search rate and query tests completed successfully!")
