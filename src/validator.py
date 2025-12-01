# File: src/validator.py
import pandera as pa
import polars as pl

# 1. Define the Rules (The Contract)
schema = pa.DataFrameSchema({
    "customer_id": pa.Column(int, required=True),
    "billing_cost": pa.Column(float, checks=pa.Check.ge(0), nullable=True), # Must be >= 0
    "age_years": pa.Column(int, checks=[
        pa.Check.ge(0),  # Age must be >= 0
        pa.Check.le(120) # Age must be <= 120
    ], coerce=True),
    "label_class": pa.Column(int, checks=pa.Check.isin([0, 1]), coerce=True),
})

class DataValidator:
    def __init__(self, df: pl.DataFrame):
        self.df = df

    def execute(self):
        """
        Validates the Polars DataFrame. 
        Drops rows that violate the logic (Self-Healing).
        """
        # Convert to Pandas for validation (Pandera standard)
        pdf = self.df.to_pandas()

        try:
            # "lazy=True" means: check ALL errors, don't just stop at the first one
            clean_pdf = schema.validate(pdf, lazy=True)
            print("✅ Data validation passed instantly.")
            return pl.from_pandas(clean_pdf)
            
        except pa.errors.SchemaErrors as err:
            print(f"⚠️  Data Violation detected! Found {len(err.failure_cases)} bad values.")
            
            # The "Self-Healing" logic:
            # We identify the index of bad rows and remove them.
            bad_indices = err.failure_cases["index"].unique()
            
            print(f"🛠️  Auto-Fixing: Dropping {len(bad_indices)} corrupt rows...")
            
            # Drop bad rows from the original dataset
            clean_pdf = pdf.drop(index=bad_indices)
            
            return pl.from_pandas(clean_pdf)

if __name__ == "__main__":
    from loader import DataLoader
    
    print("Testing Validator with BAD data...")
    # 1. Load the data (which has the -1 Age)
    loader = DataLoader("data/raw/input_data.csv")
    raw_df = loader.execute()
    
    # 2. Validate and Fix
    validator = DataValidator(raw_df)
    final_df = validator.execute()
    
    print("\n--- Final 'Golden' Dataset ---")
    print(final_df)