#!/bin/bash

# Count total lines (excluding header)
total=$(tail -n +2 asset_ids.csv | wc -l)
per_file=$((total / 8))

echo "Total IDs: $total"
echo "Per file: ~$per_file"

# Split the file (skip header, then split)
tail -n +2 asset_ids.csv | split -l $per_file -d -a 1 - temp_

# Add header and rename to proper files
for i in {0..7}; do
  num=$((i + 1))
  echo "asset_id" > asset_ids_${num}.csv
  cat temp_${i} >> asset_ids_${num}.csv
  echo "Created asset_ids_${num}.csv ($(wc -l < asset_ids_${num}.csv) lines)"
  rm temp_${i}
done

echo "✅ Done! Created 8 CSV files."
