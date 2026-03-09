import requests
import time
import json
import os

START_PAGE = 100
END_PAGE = 120

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

# make folders
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

# make pages in json
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
def main():
    print(f"Pages: {START_PAGE} to {END_PAGE - 1}")
    download_pages(START_PAGE, END_PAGE)

    print("\nDone!")

if __name__ == "__main__":
    main()