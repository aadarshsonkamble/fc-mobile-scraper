#!/bin/bash

# Count total missing IDs (excluding header)
total=$(tail -n +2 missing_ids.csv | wc -l)
per_file=$((total / 17))

echo "Total missing IDs: $total"
echo "Per file: ~$per_file"

# Split the file
tail -n +2 missing_ids.csv | split -l $per_file -d -a 2 - temp_

# Add header and rename
counter=1
for file in temp_*; do
  echo "asset_id" > missing_ids_${counter}.csv
  cat $file >> missing_ids_${counter}.csv
  lines=$(wc -l < missing_ids_${counter}.csv)
  echo "Created missing_ids_${counter}.csv ($lines lines)"
  rm $file
  counter=$((counter + 1))
done

echo "✅ Done! Created 17 CSV files with missing IDs."
