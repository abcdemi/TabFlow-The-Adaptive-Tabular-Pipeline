# File: src/loader.py
import polars as pl
import os

class DataLoader:
    def __init__(self, source_path):
        self.source_path = source_path
        self.dataset = None

    def execute(self):
        """Runs the complete loading sequence."""
        self._read_file()
        self._normalize_headers()
        self._fix_types()
        return self.dataset

    def _read_file(self):
        """Loads the raw CSV into memory."""
        if not os.path.exists(self.source_path):
            raise FileNotFoundError(f"Missing file: {self.source_path}")
        
        self.dataset = pl.read_csv(self.source_path)

    def _normalize_headers(self):
        """
        Converts 'Billing Cost' -> 'billing_cost'.
        Ensures downstream code doesn't break because of a capital letter.
        """
        if self.dataset is not None:
            # 1. Lowercase all names
            # 2. Replace spaces with underscores
            clean_headers = [c.lower().replace(" ", "_") for c in self.dataset.columns]
            
            # Create a dictionary to map Old -> New
            rename_map = dict(zip(self.dataset.columns, clean_headers))
            self.dataset = self.dataset.rename(rename_map)

    def _fix_types(self):
        """
        Cleans the 'billing_cost' column:
        1. Removes '$' signs.
        2. Converts to Float.
        3. Sets 'INVALID' text to null (strict=False).
        """
        if "billing_cost" in self.dataset.columns:
            self.dataset = self.dataset.with_columns(
                pl.col("billing_cost")
                .str.replace(r"\$", "")          # Remove currency symbols
                .cast(pl.Float64, strict=False)  # Convert to numbers (errors become null)
            )

if __name__ == "__main__":
    # Internal Unit Test
    # This runs only when you call 'python src/loader.py' directly
    print("Testing Loader...")
    loader = DataLoader("data/raw/input_data.csv")
    clean_df = loader.execute()
    
    print(clean_df)