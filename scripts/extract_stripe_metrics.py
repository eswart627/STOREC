import csv
import os

def extract_stripe_metrics(input_csv, output_csv):
    if not os.path.exists(input_csv):
        print(f"Error: {input_csv} not found")
        return

    # Relevant columns for the plot
    # Note: CSV might have leading/trailing spaces in headers
    target_cols = [
        'mode', 'size_mb', 
        'total_stripe_build_time', 'total_encoding_time',
        'avg_stripe_build_time', 'avg_encoding_time_per_stripe'
    ]

    extracted_data = []
    
    with open(input_csv, mode='r', newline='') as f:
        # Use DictReader which handles headers. 
        # We'll strip keys and values to handle the whitespace in the current metrics.csv
        reader = csv.DictReader(f)
        for row in reader:
            # Clean the row keys and values
            clean_row = {k.strip(): v.strip() for k, v in row.items()}
            
            try:
                size_mb = float(clean_row['size_mb'])
                if size_mb < 1.0 or size_mb > 64.0:
                    continue
                
                # Extract relevant fields
                new_entry = {col: clean_row[col] for col in target_cols if col in clean_row}
                
                # Calculate derived metrics used in plotting
                tsbt = float(clean_row.get('total_stripe_build_time', 0))
                tet = float(clean_row.get('total_encoding_time', 0))
                asbt = float(clean_row.get('avg_stripe_build_time', 0))
                aetps = float(clean_row.get('avg_encoding_time_per_stripe', 0))
                
                new_entry['total_time'] = str(tsbt + tet)
                new_entry['avg_time'] = str(asbt + aetps)
                
                extracted_data.append(new_entry)
            except (ValueError, KeyError) as e:
                print(f"Skipping row due to error: {e}")
                continue

    if not extracted_data:
        print("No data extracted.")
        return

    # Sort by size and then mode
    extracted_data.sort(key=lambda x: (float(x['size_mb']), x['mode']))

    # Write to output CSV
    headers = list(extracted_data[0].keys())
    with open(output_csv, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(extracted_data)
        
    print(f"Successfully extracted stripe metrics to {output_csv}")

if __name__ == "__main__":
    extract_stripe_metrics('metrics.csv', 'stripe_encoding_metrics.csv')
