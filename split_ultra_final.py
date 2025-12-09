import csv

with open('ultra_final_missing_ids.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)  # Skip header
    ids = [row[0] for row in reader]

per_file = len(ids) // 10

for i in range(10):
    start = i * per_file
    end = start + per_file if i < 9 else len(ids)
    
    with open(f'ultra_final_missing_{i+1}.csv', 'w', newline='') as out:
        writer = csv.writer(out)
        writer.writerow(['asset_id'])
        for asset_id in ids[start:end]:
            writer.writerow([asset_id])
    
    print(f'Created ultra_final_missing_{i+1}.csv with {end - start} IDs')
