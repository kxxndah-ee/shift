import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'C:/Users/user/.gemini/antigravity/scratch/media_personnel_tracker')

from data_pipeline import MediaDatabase

db = MediaDatabase()
print("=== Query Test: search_records(media='헤럴드경제') ===")
herald_all = db.search_records(media="헤럴드경제")
print(f"Result count: {len(herald_all)}건")
years = [r['pub_date'][:4] for r in herald_all if r.get('pub_date')]
from collections import Counter
for yr, cnt in sorted(Counter(years).items()):
    print(f"  {yr}: {cnt}건")

print("\n=== Query Test: search_records(media='헤럴드경제', dept='건설부동산') ===")
const_re = db.search_records(media="헤럴드경제", dept="건설부동산")
print(f"Result count: {len(const_re)}건")
for r in const_re[:5]:
    print(f"  {r['pub_date']} | {r['name']} | {r['dept']} | {r['rank_title']}")

print("\n=== Query Test: search_reporters(media='헤럴드경제') ===")
reps = db.search_reporters(media="헤럴드경제")
print(f"Result count: {len(reps)}명")
