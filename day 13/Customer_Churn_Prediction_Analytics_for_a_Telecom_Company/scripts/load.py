# ===========================
# load.py
# ===========================
# Purpose: Load transformed telco_customer_churn_data into Supabase

import os
import math
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
# Table creation note (no RPC)
# ------------------------------------------------------
def create_table_if_not_exists():
    """
    Reminder: Create the table once in Supabase SQL Editor.

    Example SQL (run in Supabase SQL Editor):

    create table if not exists public.telco_customer_churn_data (
        id bigserial primary key,
        tenure integer,
        monthlycharges double precision,
        totalcharges double precision,
        churn text,
        internetservice text,
        contract text,
        paymentmethod text,
        tenure_group text,
        monthly_charge_segment integer,
        has_internet_service integer,
        is_multi_line_user integer,
        contract_type_code integer
    );

    After creating or changing the table, run:
    notify pgrst, 'reload schema';
    """
    print("ℹ️  Make sure 'telco_customer_churn_data' table exists in Supabase (created via SQL Editor).")


# ------------------------------------------------------
# Helper: convert non-finite floats to None
# ------------------------------------------------------
def clean_record_floats(record: dict) -> dict:
    """
    Ensure all float-like values in a record are JSON-compliant:
    convert NaN / inf / -inf to None.
    """
    cleaned = {}
    for k, v in record.items():
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                cleaned[k] = None
            else:
                cleaned[k] = v
        else:
            cleaned[k] = v
    return cleaned


# ------------------------------------------------------
# Load CSV data into Supabase table
# ------------------------------------------------------
def load_to_supabase(staged_path: str, table_name: str = "telco_customer_churn_data"):
    """
    Load a transformed CSV into a Supabase table.

    Args:
        staged_path (str): Path to the transformed CSV file.
        table_name (str): Supabase table name. Default is 'telco_customer_churn_data'.
    """
    # Convert to absolute path relative to this file
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(os.path.join(os.path.dirname(__file__), staged_path))

    print(f"🔍 Looking for data file at: {staged_path}")

    if not os.path.exists(staged_path):
        print(f"❌ Error: File not found at {staged_path}")
        print("ℹ️  Please run transform.py first to generate the transformed data")
        return

    try:
        supabase = get_supabase_client()

        # Read full CSV
        df = pd.read_csv(staged_path)
        print(f"📄 CSV loaded with {len(df)} rows.")
        print("Original columns:", df.columns.tolist())

        # Keep only the columns that exist in the table (lowercase schema version)
        df = df[
            [
                "tenure",
                "monthlycharges",
                "totalcharges",
                "churn",
                "internetservice",
                "contract",
                "paymentmethod",
                "tenure_group",
                "monthly_charge_segment",
                "has_internet_service",
                "is_multi_line_user",
                "contract_type_code",
            ]
        ]

        print("Filtered columns for insert:", df.columns.tolist())
        print("Dtypes before cleaning:")
        print(df.dtypes)

        # Ensure numeric columns are really numeric (coerce bad values to NaN)
        numeric_cols = [
            "tenure",
            "monthlycharges",
            "totalcharges",
            "monthly_charge_segment",
            "has_internet_service",
            "is_multi_line_user",
            "contract_type_code",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Replace inf and -inf with NaN then NaN with None
        df = df.replace([float("inf"), float("-inf")], pd.NA)
        df = df.where(pd.notnull(df), None)

        print("Null counts after cleaning:")
        print(df.isnull().sum())

        # Show a few rows to verify values (especially contract_type_code)
        print("Sample rows after cleaning:")
        print(df.head(5))

        total_rows = len(df)
        print(f"📊 Loading {total_rows} rows into '{table_name}'...")

        batch_size = 50
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i : i + batch_size].copy()

            # Convert DataFrame rows to dicts and clean float values per record
            records = [clean_record_floats(rec) for rec in batch.to_dict("records")]

            # Debug: show first record of this batch once
            if i == 0 and records:
                print("First record being inserted (after cleaning):")
                print(records[0])

            try:
                response = supabase.table(table_name).insert(records).execute()
                error_attr = getattr(response, "error", None)
                if error_attr:
                    print(f"⚠️  Error in batch {i // batch_size + 1}: {error_attr}")
                else:
                    end = min(i + batch_size, total_rows)
                    print(f"✅ Inserted rows {i + 1}-{end} of {total_rows}")
            except Exception as e:
                print(f"⚠️  Error in batch {i // batch_size + 1}: {str(e)}")
                continue

        print(f"🎯 Finished loading data into '{table_name}'.")

    except Exception as e:
        print(f"❌ Error loading data: {e}")


# ------------------------------------------------------
# Run as standalone script
# ------------------------------------------------------
if __name__ == "__main__":
    # Path relative to this script file
    staged_csv_path = os.path.join("..", "data", "staged", "churn_transformed.csv")

    # Just a reminder message; real table creation is via SQL Editor
    create_table_if_not_exists()

    load_to_supabase(staged_csv_path)
