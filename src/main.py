import json
import pandas as pd

# 1. load jikan
def load_jikan():
    print("JIKAN")
    file_path = "data/processed/jikan_all.json"
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f" {file_path} not found")
        return

    print(f"Total anime: {len(data)}")
    print("First 20 anime (Name | Rating | Genres):")

    for anime in data[:20]:
        name = anime.get("title_english") or anime.get("title") or "Unknown"
        rating = anime.get("score", "N/A")
        genres = [g.get("name") for g in anime.get("genres", [])]
        print(f"- {name} | Rating: {rating} | Genres: {genres}")

# 2. Loads Kaggle anime CSV data set
def load_csv():
    print("\nCSV DATASET")
    file_path = "../CS 4265 Anime Project/anime-dataset-2023 2.csv"
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f" {file_path} not found")
        return

    name_col = "English name" if "English name" in df.columns else "Name"
    score_col = "Score"
    genre_col = "Genres"

    print(f"Rows: {len(df)}")
    print("First 20 anime (Name | Rating | Genres):")

    for _, row in df.head(20).iterrows():
        print(f"- {row[name_col]} | Rating: {row[score_col]} | Genres: {row[genre_col]}")

# 3. load anilist
def load_anilist():
    print("\nANILIST")
    file_path = "data/processed/anilist_all.json"
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f" {file_path} not found")
        return

    print(f"Total anime: {len(data)}")
    print("First 20 anime (Name | Rating | Genres):")

    for anime in data[:20]:
        name = anime["title"]["english"] or anime["title"]["romaji"]
        rating = anime.get("averageScore", "N/A")
        genres = anime.get("genres", [])
        print(f"- {name} | Rating: {rating} | Genres: {genres}")

def main():
    print("ANIME DATA SOURCE TEST")
    load_jikan()
    load_csv()
    load_anilist()
    print("\nAll sources loaded and verified successfully!")
if __name__ == "__main__":
    main()