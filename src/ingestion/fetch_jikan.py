import requests
import time
import json
import os

def fetch_jikan(start_page=1, end_page=801):
    RAW_DIR = "data/raw/jikan"
    os.makedirs(RAW_DIR, exist_ok=True)
    print(f"\n[JIKAN] Starting Fetch: Pages {start_page} to {end_page - 1}")

    for page in range(start_page, end_page):
        filename = os.path.join(RAW_DIR, f"jikan_page_{page}.json")

        # Skip if file already exists
        if os.path.exists(filename):
            continue

        url = f"https://api.jikan.moe/v4/anime?page={page}"
        success = False
        retries = 0

        while not success and retries < 5:
            try:
                res = requests.get(url)

                # Handle Rate Limiting
                if res.status_code == 429:
                    print(f"  [!] Rate Limit hit on page {page}. Cooling down 30s...")
                    time.sleep(30)
                    retries += 1
                    continue

                res.raise_for_status()
                full_response = res.json()
                data = full_response.get("data", [])
                pagination = full_response.get("pagination", {})

                # Save the data
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)

                print(f"  [✓] Saved page {page} ({len(data)} anime)")

                # Check if there is a next page
                if not pagination.get("has_next_page", False):
                    print("[INFO] Reached the end of Jikan database.")
                    return

                success = True
                time.sleep(1.5)

            except Exception as e:
                print(f"  [ERROR] Page {page}: {e}")
                retries += 1
                time.sleep(5)
    print("[INFO] Jikan Fetch Task Complete.")