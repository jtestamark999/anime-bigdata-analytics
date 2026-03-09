import json
import glob
import os

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

def merge_anilist():
    all_ani = []
    # Find all files that start with anilist_page_
    files = glob.glob(os.path.join(RAW_DIR, "anilist_page_*.json"))

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            all_ani.extend(data)

    output_path = os.path.join(PROCESSED_DIR, "anilist_all.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_ani, f, indent=2)
    print(f"Created {output_path} with {len(all_ani)} records.")

def merge_jikan():
    all_jikan = []
    # Find all files that start with jikan_page_
    files = glob.glob(os.path.join(RAW_DIR, "jikan_page_*.json"))

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            all_jikan.extend(data)

    output_path = os.path.join(PROCESSED_DIR, "jikan_all.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_jikan, f, indent=2)
    print(f"Created {output_path} with {len(all_jikan)} records.")

if __name__ == "__main__":
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    merge_anilist()
    merge_jikan()