import sys
import json
import sqlite3
import gzip
from collections import Counter

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker')

from data_pipeline import (
    MediaDatabase,
    PersonnelTrackerEngine,
    normalize_media_name,
    is_same_media_company,
    is_media_organization,
    is_excluded_department
)

db_path = "C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/media_tracker.db"
seed_path = "C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/data/archive_seed.json.gz"

print("=== Ingesting Clean Herald Historical Archive ===")

# 1. Load clean parsed records
parsed_path = 'C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/scripts/herald_parsed_records.json'
with open(parsed_path, encoding='utf-8') as f:
    raw_new_recs = json.load(f)

clean_records = []
for r in raw_new_recs:
    name = r.get("name", "").strip()
    if name in ["보기", "기자", "대표", "부장", "차장", "국장", "실장", "회장", "위원", "헤럴드", "코리아", "뉴스", "음성", "영상", "서비스", "기자상", "기자연수", "주재", "사무", "단골이", "될까요", "그룹"]:
        continue
    if len(name) < 2 or len(name) > 4:
        continue
    if is_excluded_department(r.get("dept", "")):
        continue
    
    raw_m = r.get("media", "").strip()
    norm_m = normalize_media_name(raw_m)
    if is_same_media_company(raw_m, "헤럴드경제") or "헤럴드" in raw_m or "herald" in raw_m.lower():
        norm_m = "헤럴드경제"
        r["media"] = "헤럴드경제"
    elif is_media_organization(norm_m):
        r["media"] = norm_m
    else:
        continue

    pdate = r.get("pub_date", "")
    if not pdate or len(pdate) < 10:
        continue

    clean_records.append(r)

print(f"Clean new records to process: {len(clean_records)}")

# 2. Process via PersonnelTrackerEngine
engine = PersonnelTrackerEngine(db_path)
stats = engine.process_records(clean_records, append=True)
print(f"Ingestion result: {stats}")

# 3. Verification & Seed Export
print("\nIngestion complete. Verifying DB stats...")

conn = sqlite3.connect(db_path)
c = conn.cursor()

c.execute("SELECT count(*) FROM personnel_records")
total_records = c.fetchone()[0]

c.execute("SELECT count(*) FROM personnel_records WHERE media = '헤럴드경제'")
herald_records = c.fetchone()[0]

c.execute("SELECT min(pub_date), max(pub_date) FROM personnel_records WHERE media = '헤럴드경제'")
min_d, max_d = c.fetchone()

c.execute("SELECT substr(pub_date, 1, 4) as yr, count(*) FROM personnel_records WHERE media = '헤럴드경제' GROUP BY yr ORDER BY yr")
yearly = c.fetchall()

c.execute("SELECT count(*) FROM reporters WHERE current_media = '헤럴드경제'")
herald_reps = c.fetchone()[0]

c.execute("SELECT count(*) FROM reporters")
total_reps = c.fetchone()[0]

c.execute("SELECT count(*) FROM job_changes")
total_changes = c.fetchone()[0]

print(f"\n=== Database Verification Summary ===")
print(f"Total Records in DB: {total_records:,}건")
print(f"Total Reporters: {total_reps:,}명")
print(f"Total Job Changes: {total_changes:,}건")
print(f"헤럴드경제 Records: {herald_records:,}건")
print(f"헤럴드경제 Tracking Period: {min_d} ~ {max_d}")
print(f"헤럴드경제 Reporters: {herald_reps:,}명")
print(f"Yearly Breakdown for 헤럴드경제:")
for yr, cnt in yearly:
    print(f"  {yr}: {cnt}건")

# 4. Export to archive_seed.json.gz
print(f"\nExporting updated master seed to {seed_path}...")
c.execute("SELECT id, pub_date, media, dept, domain, rank_title, rank_level, name, action_type, email, raw_text, reporter_id, article_title, article_url FROM personnel_records ORDER BY pub_date ASC, id ASC")
cols = [col[0] for col in c.description]
all_rows = [dict(zip(cols, row)) for row in c.fetchall()]

with gzip.open(seed_path, "wt", encoding="utf-8") as gf:
    json.dump(all_rows, gf, ensure_ascii=False)

print(f"Master seed successfully exported: {len(all_rows):,} records saved to archive_seed.json.gz")
conn.close()
