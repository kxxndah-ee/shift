import sys
import sqlite3

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker')

from data_pipeline import MediaDatabase, are_group_affiliates, is_same_media_company

db = MediaDatabase()

print("=== 1. Identity & Affiliation Tests ===")
print("is_same_media_company('헤럴드', '헤럴드미디어'):", is_same_media_company('헤럴드', '헤럴드미디어'))
print("is_same_media_company('헤럴드미디어', '(주)헤럴드'):", is_same_media_company('헤럴드미디어', '(주)헤럴드'))
print("are_group_affiliates('헤럴드', '헤럴드경제'):", are_group_affiliates('헤럴드', '헤럴드경제'))
print("are_group_affiliates('헤럴드미디어', '코리아헤럴드'):", are_group_affiliates('헤럴드미디어', '코리아헤럴드'))
print("are_group_affiliates('헤럴드경제', '코리아헤럴드'):", are_group_affiliates('헤럴드경제', '코리아헤럴드'))

print("\n=== 2. Search Query Tests ===")
res_herald = db.search_records(media="헤럴드")
print(f"search_records(media='헤럴드') result count: {len(res_herald):,}건 (산하 전체)")

res_hm = db.search_records(media="헤럴드미디어")
print(f"search_records(media='헤럴드미디어') result count: {len(res_hm):,}건 (산하 전체)")

res_he = db.search_records(media="헤럴드경제")
print(f"search_records(media='헤럴드경제') result count: {len(res_he):,}건 (헤럴드경제+본사공통)")

res_kh = db.search_records(media="코리아헤럴드")
print(f"search_records(media='코리아헤럴드') result count: {len(res_kh):,}건 (코리아헤럴드+본사공통)")

print("\n=== 3. Column Names of job_changes ===")
with db.get_connection() as conn:
    c = conn.cursor()
    c.execute("PRAGMA table_info(job_changes)")
    cols = [r[1] for r in c.fetchall()]
    print("Columns:", cols)
    c.execute(f"SELECT * FROM job_changes WHERE (from_media LIKE '%헤럴드%' OR to_media LIKE '%헤럴드%') LIMIT 5")
    rows = c.fetchall()
    print(f"Found {len(rows)} job change samples involving Herald:")
    for r in rows:
        row_dict = dict(zip(cols, r))
        print(f"  Reporter {row_dict.get('reporter_id')}: {row_dict.get('from_media')} -> {row_dict.get('to_media')} ({row_dict.get('change_type')}) on {row_dict.get('change_date')}")
