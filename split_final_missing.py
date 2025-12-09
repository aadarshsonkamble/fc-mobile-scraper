import csv

with open('final_missing_ids.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)  # Skip header
    ids = [row[0] for row in reader]

per_file = len(ids) // 7

for i in range(7):
    start = i * per_file
    end = start + per_file if i < 6 else len(ids)
    
    with open(f'final_missing_{i+1}.csv', 'w', newline='') as out:
        writer = csv.writer(out)
        writer.writerow(['asset_id'])
        for asset_id in ids[start:end]:
            writer.writerow([asset_id])
    
    print(f'Created final_missing_{i+1}.csv with {end - start} IDs')
