# Module 2 Homework: Workflow Orchestration with Kestra

This repository contains the solution for the **Data Engineering Zoomcamp – Module 2 Homework**. The assignment focuses on extending existing data pipelines to process **Green** and **Yellow** taxi data for the year **2021**, performing backfills with **Kestra**, and validating results using **Python**.

---

## 📌 Assignment Overview

The objectives of this homework were to:

1. **Extend ETL workflows** to process NYC taxi datasets for 2021.
2. **Backfill historical data** using Kestra for the period **2021-01-01 → 2021-07-31**.
3. **Analyze datasets** to answer quiz questions related to file sizes, row counts, and Kestra configuration.

---

## 🛠️ Solution Implementation

### 1. Data Analysis Script (Python)

To accurately answer the quiz questions regarding **file sizes** and **row counts**—without manually downloading files via the UI—a Python script was created.

The script:

* Streams compressed CSV files directly from the source
* Decompresses them in memory
* Computes row counts and uncompressed file sizes

This approach avoids unnecessary disk usage and ensures reproducibility.

#### Code Used for Analysis

```python
import requests
import pandas as pd
import gzip
import io

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

def get_row_count(service, year, months):
    total_rows = 0
    print(f"--- Processing {service} taxi data for {year} ---")
    for month in months:
        month_str = f"{month:02d}"
        url = f"{BASE_URL}/{service}/{service}_tripdata_{year}-{month_str}.csv.gz"
        try:
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
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
        file_content = f.read()
        size_mb = len(file_content) / (1024 * 1024)
        print(f"Uncompressed size: {size_mb:.1f} MiB\n")

# --- Execution for Quiz Answers ---

# Q1: Yellow Taxi 2020-12 uncompressed size
check_file_size_uncompressed('yellow', 2020, 12)

# Q3: Yellow Taxi 2020 total rows
get_row_count('yellow', 2020, range(1, 13))

# Q4: Green Taxi 2020 total rows
get_row_count('green', 2020, range(1, 13))

# Q5: Yellow Taxi 2021-03 rows
get_row_count('yellow', 2021, [3])
```

---

### 2. Kestra Workflow Configuration

Key workflow and orchestration details:

* **Backfill Strategy:** Kestra UI → *Backfill* option on the `Schedule` trigger
* **Time Range:** `2021-01-01` to `2021-07-31`
* **Datasets:** Separate backfills executed for:

  * `taxi: yellow`
  * `taxi: green`
* **Timezone Configuration:**

  * Used IANA-compliant timezone: `America/New_York`

This ensured consistent scheduling behavior aligned with NYC taxi data timestamps.

---

## 📝 Quiz Answers

| Question                        | Answer                         | Explanation                                                                             |
| ------------------------------- | ------------------------------ | --------------------------------------------------------------------------------------- |
| **1. Yellow 2020-12 File Size** | **128.3 MiB**                  | Determined by fully decompressing the `.csv.gz` file in memory and measuring byte size. |
| **2. Rendered Variable**        | **green_tripdata_2020-04.csv** | Rendered from: `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv`.         |
| **3. Yellow 2020 Rows**         | **24,648,499**                 | Sum of rows across all 12 Yellow taxi files for 2020.                                   |
| **4. Green 2020 Rows**          | **1,734,051**                  | Sum of rows across all 12 Green taxi files for 2020.                                    |
| **5. Yellow 2021-03 Rows**      | **1,925,152**                  | Exact row count from the March 2021 Yellow taxi dataset.                                |
| **6. Timezone Config**          | **America/New_York**           | Kestra requires IANA timezone identifiers (not `EST` or UTC offsets).                   |

---

## 🔗 Links

* **Repository:** [https://github.com/LarVinchi/module_2_orchestraiton.git](https://github.com/LarVinchi/module_2_orchestraiton.git)
* **Data Source:** [https://github.com/DataTalksClub/nyc-tlc-data](https://github.com/DataTalksClub/nyc-tlc-data)

---

## ✅ Notes

* All calculations are reproducible using the provided Python script.
* No manual file downloads were required.
* The solution adheres strictly to the homework instructions and Kestra best practices.

