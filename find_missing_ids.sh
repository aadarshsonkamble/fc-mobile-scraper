#!/bin/bash

echo "🔍 Finding missing player IDs..."

# Create a temporary file for all original IDs
cat asset_ids_1.csv asset_ids_2.csv asset_ids_3.csv asset_ids_4.csv asset_ids_5.csv asset_ids_6.csv asset_ids_7.csv asset_ids_8.csv | grep -v "asset_id" | sort -u > all_original_ids.txt

# Create a temporary file for all scraped IDs
cat players_stats_1.csv players_stats_2.csv players_stats_3.csv players_stats_4.csv players_stats_5.csv players_stats_6.csv players_stats_7.csv players_stats_8.csv | grep -v "playerid" | cut -d',' -f1 | sort -u > all_scraped_ids.txt

# Find the difference (missing IDs)
comm -23 all_original_ids.txt all_scraped_ids.txt > missing_ids_temp.txt

# Count them
total_original=$(wc -l < all_original_ids.txt)
total_scraped=$(wc -l < all_scraped_ids.txt)
total_missing=$(wc -l < missing_ids_temp.txt)

echo "✅ Total original IDs: $total_original"
echo "✅ Already scraped: $total_scraped"
echo "❌ Missing IDs: $total_missing"

# Create CSV with header
echo "asset_id" > missing_ids.csv
cat missing_ids_temp.txt >> missing_ids.csv

# Cleanup
rm all_original_ids.txt all_scraped_ids.txt missing_ids_temp.txt

echo "✅ Created missing_ids.csv with $total_missing IDs"
