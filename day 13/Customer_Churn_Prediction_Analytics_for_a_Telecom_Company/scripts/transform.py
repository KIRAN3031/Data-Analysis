# ===========================
# transform.py
# ===========================
 
import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
 
# Purpose: Clean and transform Titanic dataset
def transform_data(raw_path):
    # Ensure the path is relative to project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # go up one level
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)
 
    df = pd.read_csv(raw_path)

    # converting "totalChrages" to numeric
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"],errors="coerce")
    
    # --- 1️⃣ Handle missing values ---
    # filling the missing values using the median
    df['tenure'] = df['tenure'].fillna(df['tenure'].median())
    df['MonthlyCharges'] = df['MonthlyCharges'].fillna(df['MonthlyCharges'].median())
    df['TotalCharges'] = df['TotalCharges'].fillna(df['TotalCharges'].median())

    # replacing the missing categorical values with "unknown"
    df['MultipleLines'] = df['MultipleLines'].fillna('Unknown')
    df['OnlineSecurity'] = df['OnlineSecurity'].fillna('Unknown')
    df['OnlineBackup'] = df['OnlineBackup'].fillna('Unknown')
   
    # --- 2️⃣ Feature engineering ---
    df['tenure_group'] = pd.cut(df['tenure'], bins=[0,12,36,60,np.inf], labels=['New','Regular','Loyal','Champion'])
    df['has_internet_service'] = np.where((df['InternetService'] == 'DSL') | (df['InternetService'] == 'Fiber optic'), 1, 0)
    df['is_mutli_line_user']= np.where(df['MultipleLines'] == 'Yes', 1, 0)
    df['contract_type_code']= df['Contract'].map({'Month-to-month':0,'One year':1,'Two year':2})


 
    # --- 3️⃣ Drop unnecessary columns ---
    df.drop(['customerID','gender'],axis=1,inplace=True)

    # --- 4️⃣ Save transformed data ---
    staged_path = os.path.join(staged_dir, "churn_transformed.csv")
    df.to_csv(staged_path, index=False)
    print(f"✅ Data transformed and saved at: {staged_path}")
    return staged_path
 
 
if __name__ == "__main__":
    from extract import extract_data
    raw_path = extract_data()
    transform_data(raw_path)
 
 