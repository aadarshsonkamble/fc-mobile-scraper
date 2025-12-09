import csv

print("🔍 Finding final 581 missing IDs...")

# Read all IDs from asset_ids.csv
with open('asset_ids.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)  # Skip header
    asset_ids = set(row[0].strip() for row in reader if row)

# Read all IDs from players_stats_ALL_COMPLETE.csv
with open('players_stats_ALL_COMPLETE.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)  # Skip header
    scraped_ids = set(row[0].strip() for row in reader if row)

# Find missing IDs
missing_ids = asset_ids - scraped_ids

print(f"📊 Total IDs in asset_ids.csv: {len(asset_ids)}")
print(f"✅ IDs in players_stats_ALL_COMPLETE.csv: {len(scraped_ids)}")
print(f"❌ Still missing: {len(missing_ids)}")

# Save to CSV
with open('ultra_final_missing_ids.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['asset_id'])
    for asset_id in sorted(missing_ids):
        writer.writerow([asset_id])

print(f"✅ Created ultra_final_missing_ids.csv with {len(missing_ids)} IDs")
