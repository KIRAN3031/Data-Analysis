# Customer Churn Prediction Analytics for a Telecom Company
    A telecom operator named TeleConnect is facing customer churn (customers leaving the service).
    Their data team wants to analyze customer behavior and produce a high-quality cleaned dataset for future ML modeling.
    You are assigned to build a full ETL pipeline:
    Extract → Transform → Load → Validate → Generate Analytics Summary.

# 1.EXTRACT (extract.py)

    Write a script that:
        Creates folder structure:
        data/raw
        data/staged
        data/processed
        import opendatasets as od
        load the dataset
        data/raw/churn_raw.csv
        Save raw CSV as:data/raw/churn_raw.csv

# 2.TRANSFORM (transform.py)
    must perform advanced transformations, not just cleaning.
    ✔ Cleaning Tasks
    Convert "TotalCharges" to numeric (dataset has spaces → become NaN).
    Fill missing numeric values using:
    Median for tenure, MonthlyCharges, TotalCharges.
    Replace missing categorical values with "Unknown".
    ✔ Feature Engineering
    Create the following new columns:
    1. tenure_group
    Based on tenure months:
    0–12   → "New"
    13–36  → "Regular"
    37–60  → "Loyal"
    60+    → "Champion"
    2. monthly_charge_segment
    MonthlyCharges < 30  → "Low"
    30–70              → "Medium"
    > 70                 → "High"
    3. has_internet_service
    Convert InternetService column:
    "DSL" / "Fiber optic" → 1
    "No" → 0
    4. is_multi_line_user
    1 if MultipleLines == "Yes"
    0 otherwise
    5. contract_type_code
    Map:
    Month-to-month → 0
    One year      → 1
    Two year      → 2
    ✔ Drop unnecessary fields
    Remove:
    customerID, gender
    ✔ Save output to:
    data/staged/churn_transformed.csv

# 3️.LOAD TO SUPABASE (load.py)
    Create a table:
    id BIGSERIAL PRIMARY KEY,
    tenure INTEGER,
    MonthlyCharges FLOAT,
    TotalCharges FLOAT,
    Churn TEXT,
    InternetService TEXT,
    Contract TEXT,
    PaymentMethod TEXT,
    tenure_group TEXT,
    monthly_charge_segment TEXT,
    has_internet_service INTEGER,
    is_multi_line_user INTEGER,
    contract_type_code INTEGER
    Load Data
    Batch size = 200 records at a time
    Convert NaN → None
    Insert with error handling + retry logic


# 4.VALIDATION SCRIPT (validate.py)
    After load, write a script that checks:
    No missing values in:
    tenure, MonthlyCharges, TotalCharges
    Unique count of rows = original dataset
    Row count matches Supabase table
    All segments (tenure_group, monthly_charge_segment) exist
    Contract codes are only {0,1,2}
    Print a validation summary.

# 5.ANALYSIS REPORT (etl_analysis.py)
    Read table from Supabase and generate:
    📊 Metrics
    Churn percentage
    Average monthly charges per contract
    Count of new, regular, loyal, champion customers
    Internet service distribution
    Pivot table: Churn vs Tenure Group
    Optional visualizations:
    Churn rate by Monthly Charge Segment
    Histogram of TotalCharges
    Bar plot of Contract types
    Save output CSV into:
    data/processed/analysis_summary.csv