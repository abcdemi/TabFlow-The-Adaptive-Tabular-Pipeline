# File: generate_mock_data.py
import polars as pl
import os
import numpy as np

def generate_messy_data():
    """
    Creates a CSV file simulating a real-world messy client upload.
    Issues included:
    1. Inconsistent Header Casing ('billing cost' vs 'Age_Years')
    2. Currency symbols in numeric fields ('$1200.50')
    3. Invalid string inputs for numbers ('INVALID')
    4. Logical data errors (Age = -1)
    """
    
    # 1. Create the 'data/raw' directory structure
    # This mimics a Data Lake landing zone
    output_dir = os.path.join("data", "raw")
    os.makedirs(output_dir, exist_ok=True)
    
    # 2. Define the mess
    data = {
        "CUSTOMER_ID": [1001, 1002, 1003, 1004, 1005],
        # 'Billing Cost' has dollar signs and text errors
        "Billing Cost": ["$1200.50", "$500.00", "INVALID", "$0.00", "$99.99"], 
        # 'Age_Years' has a logic error (-1)
        "Age_Years": [34, 29, -1, 52, 44], 
        # 'Label_Class' is clean (0 or 1)
        "Label_Class": [0, 1, 0, 1, 0]
    }

    # 3. Save as CSV
    df = pl.DataFrame(data)
    file_path = os.path.join(output_dir, "input_data.csv")
    df.write_csv(file_path)

    print(f"✅ Success! Messy data generated at: {file_path}")
    print("Preview of the generated mess:")
    print(df)

if __name__ == "__main__":
    generate_messy_data()