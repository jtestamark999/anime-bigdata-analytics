import pandas as pd

def verify_csv():
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

if __name__ == "__main__":
    verify_csv()