import requests
import time
import json
import os

START_PAGE = 1
END_PAGE = 50

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

def download_pages(start, end):
    print("\n Downloading AniList Pages \n")
    url = "https://graphql.anilist.co"
    query = """
    query ($page: Int, $perPage: Int) {
      Page(page: $page, perPage: $perPage) {
        pageInfo {
          hasNextPage
        }
        media(type: ANIME, sort: POPULARITY_DESC) {
          id
          idMal
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
          trending
          genres
          tags {
            name
            rank
          }
          description
          coverImage {
            large
          }
          studios {
            nodes {
              name
            }
          }
        }
      }
    }
    """
    for page in range(start, end):
        filename = f"{RAW_DIR}/anilist_page_{page}.json"
        if os.path.exists(filename):
            print(f"Page {page} already exists, skipping.")
            continue

        variables = {
            "page": page,
            "perPage": 50
        }
        response = requests.post(url, json={"query": query,
                                            "variables": variables})

        if response.status_code != 200:
            print(f"Error on page {page}: {response.text}")
            break

        result = response.json()
        data = result["data"]["Page"]["media"]
        has_next = result["data"]["Page"]["pageInfo"]["hasNextPage"]

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        print(f"Saved page {page} ({len(data)} anime)")

        time.sleep(1)
def main():
    print(f"Pages: {START_PAGE} to {END_PAGE - 1}")
    download_pages(START_PAGE, END_PAGE)

if __name__ == "__main__":
    main()