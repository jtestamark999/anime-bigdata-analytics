import os
import pandas as pd

def fetch_kaggle():
    """
    Validates the existence of the Kaggle CSV dataset and performs a
    preliminary data integrity check for the pipeline.
    """
    # Using os.path.join to handle potential space/versioning issues in filenames
    # Ensure this matches your actual file name in data/raw/
    file_path = os.path.join("data", "raw", "anime-dataset-2023 2.csv")

    print(f"\n[KAGGLE] Starting Ingestion Check: {file_path}")

    try:
        # Load briefly to verify structure and count
        df = pd.read_csv(file_path)

        total_rows = len(df)
        print(f"  [✓] File Found and Validated.")
        print(f"  [INFO] Total Records for Processing: {total_rows:,}")

        print("[INFO] Kaggle Ingestion Check Complete.")
        return True

    except FileNotFoundError:
        print(f"  [ERROR] Kaggle file not found at: {file_path}")
        print("  [HELP] Please ensure the CSV is located in the 'data/raw/' directory.")
        return False
    except Exception as e:
        print(f"  [ERROR] Failed to read Kaggle CSV: {e}")
        return False

