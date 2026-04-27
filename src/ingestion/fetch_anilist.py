import requests
import time
import json
import os

def fetch_anilist(start_page=1, end_page=401):
    RAW_DIR = "data/raw/anilist"
    os.makedirs(RAW_DIR, exist_ok=True)
    url = "https://graphql.anilist.co"

    # GraphQL Query for structured anime data
    query = """
    query ($page: Int, $perPage: Int) {
      Page(page: $page, perPage: $perPage) {
        pageInfo {
          hasNextPage
        }
        media(type: ANIME, sort: POPULARITY_DESC) {
          id
          idMal
          source
          title {
            romaji
            english
            native
          }
          format
          status
          episodes
          duration
          season
          seasonYear
          averageScore
          popularity
          favourites
          genres
          studios {
            nodes {
              name
            }
          }
        }
      }
    }
    """
    print(f"\n[ANILIST] Starting Fetch: Pages {start_page} to {end_page - 1}")

    for page in range(start_page, end_page):
        filename = os.path.join(RAW_DIR, f"anilist_page_{page}.json")

        # Skip if file already exists
        if os.path.exists(filename):
            continue

        variables = {
            "page": page,
            "perPage": 50
        }

        success = False
        while not success:
            try:
                response = requests.post(url, json={"query": query, "variables": variables})

                # Handles AniList's Rate Limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    print(f" [!] Rate Limit Hit! Cooling down for {retry_after}s...")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                result = response.json()

                page_data = result.get("data", {}).get("Page", {})
                media_list = page_data.get("media", [])
                has_next = page_data.get("pageInfo", {}).get("hasNextPage", False)

                # Save the JSON data
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(media_list, f, indent=2)

                print(f"  [✓] Saved page {page} ({len(media_list)} anime)")

                if not has_next:
                    print("[INFO] No more pages available. Stopping.")
                    return

                success = True
                time.sleep(1.5)

            except Exception as e:
                print(f"  [ERROR] Critical Error on page {page}: {e}")
                print("  Waiting 10 seconds before retrying...")
                time.sleep(10)
    print("[INFO] AniList Fetch Task Complete.")