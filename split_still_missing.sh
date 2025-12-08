#!/bin/bash

# Count total still-missing IDs (excluding header)
total=$(tail -n +2 still_missing_ids.csv | wc -l)
per_file=$((total / 10))

echo "Total still-missing IDs: $total"
echo "Per file: ~$per_file"

# Split the file
tail -n +2 still_missing_ids.csv | split -l $per_file -d -a 2 - temp_

# Add header and rename
counter=1
for file in temp_*; do
  if [ $counter -le 10 ]; then
    echo "asset_id" > still_missing_${counter}.csv
    cat $file >> still_missing_${counter}.csv
    lines=$(wc -l < still_missing_${counter}.csv)
    echo "Created still_missing_${counter}.csv ($lines lines)"
    rm $file
    counter=$((counter + 1))
  else
    # Merge extra files into the 10th file
    tail -n +1 $file >> still_missing_10.csv
    rm $file
  fi
done

echo "✅ Done! Created 10 CSV files with still-missing IDs."
