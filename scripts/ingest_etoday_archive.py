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
    is_excluded_department
)

db_path = "C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/media_tracker.db"
seed_path = "C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/data/archive_seed.json.gz"

print("=== Ingesting Clean etoday Historical Archive ===")

# 1. Load clean parsed records
with open('C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker/scripts/etoday_parsed_records.json', encoding='utf-8') as f:
    raw_new_recs = json.load(f)

# Filter out bogus records (e.g. hani Hyundai motor group, UI buttons, etc.)
clean_records = []
for r in raw_new_recs:
    if '329857.html' in r.get('article_url', ''):
        if r.get('name') == '이도훈':
            clean_records.append(r)
        continue
    name = r.get("name", "").strip()
    if name in ["단골이", "될까요", "음성", "영상", "서비스", "기자상", "기자연수", "조국", "그룹"]:
        continue
    if is_excluded_department(r.get("dept", "")):
        continue
    r["media"] = "이투데이"
    clean_records.append(r)

print(f"Clean new records to process: {len(clean_records)}")

# 2. Process via PersonnelTrackerEngine
engine = PersonnelTrackerEngine(db_path)
stats = engine.process_records(clean_records, append=True)
print(f"Ingestion result: {stats}")

# 3. Verification & Seed Export
print("Ingestion complete. Verifying DB stats...")

# 4. Verify DB statistics
conn = sqlite3.connect(db_path)
c = conn.cursor()

c.execute("SELECT count(*) FROM personnel_records")
total_records = c.fetchone()[0]

c.execute("SELECT count(*) FROM personnel_records WHERE media LIKE '%이투데이%'")
etoday_records = c.fetchone()[0]

c.execute("SELECT min(pub_date), max(pub_date) FROM personnel_records WHERE media LIKE '%이투데이%'")
min_d, max_d = c.fetchone()

c.execute("SELECT substr(pub_date, 1, 4) as yr, count(*) FROM personnel_records WHERE media LIKE '%이투데이%' GROUP BY yr ORDER BY yr")
yearly = c.fetchall()

c.execute("SELECT count(*) FROM reporters WHERE current_media LIKE '%이투데이%'")
etoday_reps = c.fetchone()[0]

c.execute("SELECT count(*) FROM reporters")
total_reps = c.fetchone()[0]

c.execute("SELECT count(*) FROM job_changes")
total_changes = c.fetchone()[0]

print(f"\n=== Database Verification Summary ===")
print(f"Total Records in DB: {total_records:,}건")
print(f"Total Reporters: {total_reps:,}명")
print(f"Total Job Changes: {total_changes:,}건")
print(f"이투데이 Records: {etoday_records:,}건")
print(f"이투데이 Tracking Period: {min_d} ~ {max_d}")
print(f"이투데이 Reporters: {etoday_reps:,}명")
print(f"Yearly Breakdown for 이투데이:")
for yr, cnt in yearly:
    print(f"  {yr}: {cnt}건")

# 5. Export to archive_seed.json.gz
print(f"\nExporting updated seed to {seed_path}...")
c.execute("SELECT id, pub_date, media, dept, domain, rank_title, rank_level, name, action_type, email, raw_text, reporter_id, article_title, article_url FROM personnel_records ORDER BY pub_date ASC, id ASC")
cols = [col[0] for col in c.description]
all_rows = [dict(zip(cols, row)) for row in c.fetchall()]

with gzip.open(seed_path, "wt", encoding="utf-8") as gf:
    json.dump(all_rows, gf, ensure_ascii=False)

print(f"Master seed successfully exported: {len(all_rows):,} records saved to archive_seed.json.gz")
conn.close()
