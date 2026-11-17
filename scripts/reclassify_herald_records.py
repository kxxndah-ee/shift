import sqlite3
import json
import gzip
import sys

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker')

from data_pipeline import (
    MediaDatabase,
    PersonnelTrackerEngine,
    normalize_media_name,
    is_same_media_company,
    are_group_affiliates,
    is_media_organization
)

db_path = "C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/media_tracker.db"
seed_path = "C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/data/archive_seed.json.gz"

print("=== 1. Loading 42,975 Records from archive_seed.json.gz ===")
with gzip.open(seed_path, "rt", encoding="utf-8") as gf:
    all_seed_records = json.load(gf)

print(f"Loaded {len(all_seed_records):,} records from seed")

reclassified_records = []
kh_count = 0
herald_parent_count = 0
he_count = 0

for r in all_seed_records:
    med = r.get("media", "").strip()
    title_s = r.get("article_title", "") or ""
    raw_s = r.get("raw_text", "") or ""
    dept_s = r.get("dept", "") or ""

    if "헤럴드" in med or "헤럴드" in title_s or "헤럴드" in raw_s:
        # 1. 코리아헤럴드
        if (
            "코리아헤럴드" in dept_s 
            or "코리아헤럴드" in raw_s 
            or ("코리아헤럴드" in title_s and "헤럴드경제" not in title_s and "헤럴드경제" not in raw_s)
        ):
            r["media"] = "코리아헤럴드"
            kh_count += 1
        # 2. 헤럴드 본사 / 지주 / 헤럴드미디어
        elif (
            (
                any(k in raw_s for k in ["헤럴드미디어", "(주)헤럴드", "주식회사 헤럴드", "헤럴드그룹"])
                or any(k in dept_s for k in ["헤럴드미디어", "(주)헤럴드", "헤럴드그룹"])
                or (any(k in title_s for k in ["헤럴드미디어", "(주)헤럴드"]) and "헤럴드경제" not in raw_s and "헤럴드경제" not in title_s)
                or (raw_s.startswith("헤럴드 ") and "헤럴드경제" not in raw_s)
            )
            and "헤럴드경제" not in raw_s 
            and "헤럴드경제" not in dept_s
        ):
            r["media"] = "헤럴드"
            herald_parent_count += 1
        # 3. 헤럴드경제
        else:
            r["media"] = "헤럴드경제"
            he_count += 1

    reclassified_records.append(r)

print(f"\nHerald Entity Classification Result:")
print(f"  코리아헤럴드: {kh_count:,}건")
print(f"  헤럴드 (법인/지주/헤럴드미디어): {herald_parent_count:,}건")
print(f"  헤럴드경제: {he_count:,}건")
print(f"  합계: {kh_count + herald_parent_count + he_count:,}건")

# Verify is_media_organization on all three
print(f"\nis_media_organization('헤럴드'): {is_media_organization('헤럴드')}")
print(f"is_media_organization('헤럴드미디어'): {is_media_organization('헤럴드미디어')}")
print(f"is_media_organization('헤럴드경제'): {is_media_organization('헤럴드경제')}")
print(f"is_media_organization('코리아헤럴드'): {is_media_organization('코리아헤럴드')}")

print("\n=== 2. Re-processing all records via PersonnelTrackerEngine ===")
engine = PersonnelTrackerEngine(db_path)
stats = engine.process_records(reclassified_records, append=False)
print(f"Engine stats: {stats}")

print("\n=== 3. Database Verification ===")
conn = sqlite3.connect(db_path)
c = conn.cursor()

c.execute("SELECT count(*) FROM personnel_records")
total_rec = c.fetchone()[0]

c.execute("SELECT count(*) FROM reporters")
total_rep = c.fetchone()[0]

c.execute("SELECT count(*) FROM job_changes")
total_jc = c.fetchone()[0]

print(f"Total Records in DB: {total_rec:,}건")
print(f"Total Reporters: {total_rep:,}명")
print(f"Total Job Changes: {total_jc:,}건")

print("\nHerald Family Breakdown in personnel_records:")
c.execute("SELECT media, count(*), min(pub_date), max(pub_date) FROM personnel_records WHERE media LIKE '%헤럴드%' GROUP BY media ORDER BY count(*) DESC")
for m, cnt, mind, maxd in c.fetchall():
    print(f"  {m:15s}: {cnt:5d}건 ({mind} ~ {maxd})")

print("\nHerald Family Reporters in reporters table:")
c.execute("SELECT current_media, count(*) FROM reporters WHERE current_media LIKE '%헤럴드%' GROUP BY current_media ORDER BY count(*) DESC")
for m, cnt in c.fetchall():
    print(f"  {m:15s}: {cnt:5d}명")

# 4. Export updated seed
print(f"\n=== 4. Exporting to {seed_path} ===")
c.execute("SELECT id, pub_date, media, dept, domain, rank_title, rank_level, name, action_type, email, raw_text, reporter_id, article_title, article_url FROM personnel_records ORDER BY pub_date ASC, id ASC")
cols = [col[0] for col in c.description]
all_rows = [dict(zip(cols, row)) for row in c.fetchall()]

with gzip.open(seed_path, "wt", encoding="utf-8") as gf:
    json.dump(all_rows, gf, ensure_ascii=False)

print(f"Master seed exported successfully: {len(all_rows):,} records saved.")
conn.close()
