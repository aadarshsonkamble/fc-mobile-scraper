#!/bin/bash

echo "🔍 Finding IDs that are STILL missing after both runs..."

# Create temp file for what SHOULD have been scraped (from missing_ids 1-8)
cat missing_ids_1.csv missing_ids_2.csv missing_ids_3.csv missing_ids_4.csv missing_ids_5.csv missing_ids_6.csv missing_ids_7.csv missing_ids_8.csv | grep -v "asset_id" | sort -u > should_have_scraped.txt

# Create temp file for what WAS scraped in OLD run (1-8)
if ls players_stats_*_OLD.csv 1> /dev/null 2>&1; then
  cat players_stats_1_OLD.csv players_stats_2_OLD.csv players_stats_3_OLD.csv players_stats_4_OLD.csv players_stats_5_OLD.csv players_stats_6_OLD.csv players_stats_7_OLD.csv players_stats_8_OLD.csv 2>/dev/null | grep -v "playerid" | cut -d',' -f1 | sort -u > scraped_old.txt
else
  touch scraped_old.txt
fi

# Create temp file for what WAS scraped in NEW run (1-17)
cat players_stats_1.csv players_stats_2.csv players_stats_3.csv players_stats_4.csv players_stats_5.csv players_stats_6.csv players_stats_7.csv players_stats_8.csv players_stats_9.csv players_stats_10.csv players_stats_11.csv players_stats_12.csv players_stats_13.csv players_stats_14.csv players_stats_15.csv players_stats_16.csv players_stats_17.csv 2>/dev/null | grep -v "playerid" | cut -d',' -f1 | sort -u > scraped_new.txt

# Combine both scraped lists
cat scraped_old.txt scraped_new.txt | sort -u > scraped_combined.txt

# Find the difference (still missing)
comm -23 should_have_scraped.txt scraped_combined.txt > still_missing_temp.txt

# Count them
total_should=$(wc -l < should_have_scraped.txt)
total_scraped=$(wc -l < scraped_combined.txt)
total_still_missing=$(wc -l < still_missing_temp.txt)

echo "📊 Should have scraped (missing_ids 1-8): $total_should"
echo "✅ Actually scraped (OLD + NEW): $total_scraped"
echo "❌ STILL missing: $total_still_missing"

# Create CSV with header
echo "asset_id" > still_missing_ids.csv
cat still_missing_temp.txt >> still_missing_ids.csv

# Cleanup
rm should_have_scraped.txt scraped_old.txt scraped_new.txt scraped_combined.txt still_missing_temp.txt

echo "✅ Created still_missing_ids.csv with $total_still_missing IDs"
