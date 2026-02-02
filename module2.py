import requests
import pandas as pd
import gzip
import io
import os

# Base URL provided in homework
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

def get_row_count(service, year, months):
    total_rows = 0
    print(f"--- Processing {service} taxi data for {year} ---")
    
    for month in months:
        # Pad month with 0 (e.g., 1 -> '01')
        month_str = f"{month:02d}"
        url = f"{BASE_URL}/{service}/{service}_tripdata_{year}-{month_str}.csv.gz"
        
        print(f"Downloading: {url}...")
        
        # Stream download to avoid memory issues
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Read directly into pandas using the gzip compression option
            # 'iterator=True' and 'chunksize' can be used if memory is tight, 
            # but for row counting, reading headers usually suffices if metadata exists, 
            # otherwise read full file.
            
            # Since we need exact row counts, we read the file.
            # Using low_memory=False to suppress type warnings
            df = pd.read_csv(url, compression='gzip', low_memory=False)
            rows = len(df)
            total_rows += rows
            print(f"Month {month_str}: {rows} rows")
            
        except Exception as e:
            print(f"Error processing {year}-{month_str}: {e}")
            
    print(f"Total rows for {service} {year}: {total_rows:,}\n")
    return total_rows

def check_file_size_uncompressed(service, year, month):
    month_str = f"{month:02d}"
    url = f"{BASE_URL}/{service}/{service}_tripdata_{year}-{month_str}.csv.gz"
    
    print(f"--- Checking size for {service} {year}-{month_str} ---")
    response = requests.get(url)
    
    # Decompress in memory
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
        file_content = f.read()
        size_bytes = len(file_content)
        size_mb = size_bytes / (1024 * 1024) # Convert to MiB
        print(f"Uncompressed size: {size_mb:.1f} MiB\n")

# --- EXECUTION ---

# Question 1: Yellow Taxi 2020-12 uncompressed size
check_file_size_uncompressed('yellow', 2020, 12)

# Question 3: Yellow Taxi 2020 total rows
get_row_count('yellow', 2020, range(1, 13))

# Question 4: Green Taxi 2020 total rows
get_row_count('green', 2020, range(1, 13))

# Question 5: Yellow Taxi 2021-03 rows
get_row_count('yellow', 2021, [3])
