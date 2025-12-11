# ===========================
# validate.py
# ===========================
# Purpose: Validate data loaded into telco_customer_churn_data

import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv


# ------------------------------------------------------
# Supabase client
# ------------------------------------------------------
def get_supabase_client() -> Client:
    """Initialize and return Supabase client."""
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

    return create_client(url, key)


# ------------------------------------------------------
# Validation logic
# ------------------------------------------------------
def validate_data(
    table_name: str = "telco_customer_churn_data",
    original_csv_path: str = os.path.join("..", "data", "staged", "churn_transformed.csv"),
):
    sb = get_supabase_client()

    # 1. Load original dataset to get original row count
    if not os.path.isabs(original_csv_path):
        original_csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), original_csv_path))

    if not os.path.exists(original_csv_path):
        print(f"❌ Original CSV not found at {original_csv_path}")
        return

    original_df = pd.read_csv(original_csv_path)
    original_row_count = len(original_df)
    print(f"📄 Original CSV rows: {original_row_count}")

    # 2. Fetch all rows from Supabase table
    #    (if the table is large, you might switch to a paginated approach)
    response = sb.table(table_name).select("*").execute()  # [web:76]
    data = response.data or []
    df = pd.DataFrame(data)

    loaded_row_count = len(df)
    print(f"📊 Rows in Supabase table '{table_name}': {loaded_row_count}")

    # 3. Check no missing values in tenure, MonthlyCharges, TotalCharges
    #    Adjust column names to match your schema (here using lowercase)
    missing_tenure = df["tenure"].isna().sum() if "tenure" in df.columns else None
    missing_monthly = df["monthlycharges"].isna().sum() if "monthlycharges" in df.columns else None
    missing_total = df["totalcharges"].isna().sum() if "totalcharges" in df.columns else None

    # 4. Check unique row count = original dataset
    #    Using all columns except the generated id
    cols_for_unique = [c for c in df.columns if c != "id"]
    unique_rows = df[cols_for_unique].drop_duplicates()
    unique_row_count = len(unique_rows)

    # 5. Check all segments exist (tenure_group, monthly_charge_segment)
    tenure_group_values = sorted(df["tenure_group"].dropna().unique()) if "tenure_group" in df.columns else []
    monthly_seg_values = sorted(df["monthly_charge_segment"].dropna().unique()) if "monthly_charge_segment" in df.columns else []

    # 6. Check contract_type_code only in {0,1,2}
    valid_contract_codes = {0, 1, 2}
    contract_codes = set(df["contract_type_code"].dropna().tolist()) if "contract_type_code" in df.columns else set()
    invalid_contract_codes = contract_codes - valid_contract_codes

    # --------------------------------------------------
    # Print validation summary
    # --------------------------------------------------
    print("\n================ VALIDATION SUMMARY ================\n")

    # Row counts
    print(f"- Original CSV row count         : {original_row_count}")
    print(f"- Supabase table row count       : {loaded_row_count}")
    print(f"- Unique rows in Supabase (no id): {unique_row_count}")
    print(f"  -> Row count match (CSV vs DB) : {original_row_count == loaded_row_count}")
    print(f"  -> Unique rows match original? : {unique_row_count == original_row_count}")

    # Missing values
    print("\n- Missing values check (should be 0):")
    print(f"  tenure           missing: {missing_tenure}")
    print(f"  monthlycharges   missing: {missing_monthly}")
    print(f"  totalcharges     missing: {missing_total}")

    # Segment coverage
    print("\n- tenure_group distinct values:")
    print(f"  {tenure_group_values}")

    print("\n- monthly_charge_segment distinct values:")
    print(f"  {monthly_seg_values}")

    # Contract code check
    print("\n- contract_type_code check:")
    print(f"  observed codes     : {sorted(contract_codes)}")
    print(f"  expected codes     : {sorted(valid_contract_codes)}")
    print(f"  invalid codes      : {sorted(invalid_contract_codes)}")
    print(f"  -> All codes valid?: {len(invalid_contract_codes) == 0}")

    print("\n====================================================\n")


# ------------------------------------------------------
# Run as standalone script
# ------------------------------------------------------
if __name__ == "__main__":
    validate_data()
