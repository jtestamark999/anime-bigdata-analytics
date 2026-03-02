import requests
import time
import json
import os
import glob

# CHANGE EACH RUN
START_PAGE = 100
END_PAGE = 120

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

# SETUP FOLDERS
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

# DOWNLOAD JIKAN PAGES IN JSON
def download_pages(start, end):
    for page in range(start, end):
        print("Downloading page", page)
        url = f"https://api.jikan.moe/v4/anime?page={page}"
        res = requests.get(url)

        if res.status_code != 200:
            print("Error on page", page)
            break

        data = res.json()["data"]
        filename = f"{RAW_DIR}/jikan_page_{page}.json"

        if os.path.exists(filename):
            print("Already exists, skipping:", filename)
            continue

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        print("Saved:", filename)
        time.sleep(1)

# MERGE ALL JSON FILES
def merge_files():
    all_anime = []
    files = glob.glob(f"{RAW_DIR}/jikan_page_*.json")
    print("Found", len(files), "files")

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            all_anime.extend(data)

    output_file = f"{PROCESSED_DIR}/jikan_all.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_anime, f, indent=2)

    print("Merged file saved:", output_file)
    print("Total anime:", len(all_anime))

def main():
    print(f"Pages: {START_PAGE} → {END_PAGE - 1}")
    download_pages(START_PAGE, END_PAGE)
    merge_files()
    print("\nDone!")

if __name__ == "__main__":
    main()