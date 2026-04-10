
from pyspark.sql import functions as F
import os
# 1. MASTER SCHEMA Definition
MASTER_SCHEMA = [
    "mal_id", "title", "title_english", "release_year", "score",
    "episodes", "duration", "favorites", "type", "status",
    "genres", "source", "season", "studios", "themes", "rank", "rating"
]
# Get the base directory 
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Point to the folders
jikan_folder = os.path.join(BASE_DIR, "data/raw/jikan/")
anilist_folder = os.path.join(BASE_DIR, "data/raw/anilist/")
kaggle_file = os.path.join(BASE_DIR, "data/raw/anime-dataset-2023 2.csv")

def clean_anilist(input_df):
    studios_list = [
        "Madhouse",
        "MAPPA", "Bones","Sunrise","Ufotable", "Wit Studio", "A-1 Pictures", "CloverWorks", "Production I.G",
        "Kyoto Animation","Toei Animation","Pierrot","J.C. Staff","Studio Ghibli", "Shaft","David Production",
        "P.A. Works","Studio Bind","Trigger", "OLM","TMS Entertainment","8bit","Lerche","Diomedea", "White Fox",
        "Silver Link", "Doga Kobo","Brain's Base","Feel", "GoHands", "Polygon Pictures", "Orange","Science SARU",
        "Studio Deen", "Manglobe", "Geno Studio", "Lay-duce","Asahi Production","Passione", "Yokohama Animation Lab"
    ]

    # 2. Normalize reference list
    normalized_target_list = [s.lower().strip() for s in studios_list]

    # 3. Initial selection and numeric standardization
    working_df = input_df.select(
        F.col("idMal").cast("double").alias("mal_id"),
        F.coalesce(F.col("title.romaji"), F.col("title.english")).cast("string").alias("title"),
        F.col("title.english").cast("string").alias("title_english"),
        F.coalesce(F.col("seasonYear"), F.lit(0)).cast("int").alias("release_year"),
        (F.coalesce(F.col("averageScore"), F.lit(0)) / 10).cast("double").alias("score"),
        F.col("episodes").cast("double"),
        F.col("duration").cast("string"),
        F.col("favourites").cast("double").alias("favorites"),
        F.col("format").cast("string").alias("type"),
        F.col("status").cast("string"),
        F.col("genres").cast("array<string>"),
        F.col("source").cast("string"),
        F.col("season").cast("string"),
        F.col("studios.nodes.name").alias("raw_studios"))

    # 4. Normalize the incoming DATA
    working_df = working_df.withColumn("normalized_data_array",F.expr("transform(raw_studios, x -> trim(lower(x)))"))

    # 5. Intersect the normalized data with the normalized list
    working_df = working_df.withColumn("matched_studios",F.array_intersect(
        F.col("normalized_data_array"),
        F.array([F.lit(x) for x in normalized_target_list])))

    # 6. Final Formatting
    working_df = working_df.withColumn("studios",
    F.when((F.size(F.col("matched_studios")) > 0), F.col("matched_studios")).otherwise(F.array(F.lit("Unknown"))))

    # Add missing columns for MASTER_SCHEMA alignment
    working_df = working_df.withColumn("themes", F.array().cast("array<string>")) \
        .withColumn("rank", F.lit(None).cast("double")) \
        .withColumn("rating", F.lit(None).cast("string")) \
        .withColumn("title", F.trim(F.col("title"))) \
        .withColumn("title_english", F.trim(F.col("title_english")))

    return working_df.select(*MASTER_SCHEMA)

def clean_jikan(input_df):

    working_df = input_df.dropDuplicates(["mal_id"])

    working_df = working_df.withColumn("rescued_year", F.year(F.to_date(F.col("aired.from")))) \
        .withColumn("release_year", F.coalesce(F.col("year"), F.col("rescued_year")).cast("int")) \
        .fillna(0, subset=["release_year"])

    working_df = working_df.select(
        F.col("mal_id").cast("double"),
        F.trim(F.col("title")).alias("title"),
        F.trim(F.col("title_english")).alias("title_english"),

        F.col("release_year"),
        F.col("score").cast("double"),
        F.col("episodes").cast("double"),
        F.col("duration").cast("string"),
        F.col("members").cast("double").alias("favorites"),
        F.col("type").cast("string"),
        F.lit(None).cast("string").alias("status"),
        F.col("genres.name").alias("genres"),
        F.col("source").cast("string"),
        F.col("season").cast("string"),
        F.col("studios.name").alias("studios"),
        F.col("themes.name").alias("themes"),
        F.col("rank").cast("double"),
        F.col("rating").cast("string")
    )
    return working_df.select(*MASTER_SCHEMA)

def clean_kaggle(input_df):

    working_df = input_df.select(
        F.col("anime_id").cast("double").alias("mal_id"),
        F.col("Name").cast("string").alias("title"),
        F.col("English name").cast("string").alias("title_english"),

        F.when(F.col("Score") == "UNKNOWN", 0.0)
        .otherwise(F.col("Score")).cast("double").alias("score"),

        F.col("Genres").alias("raw_genres"),
        F.col("Type").cast("string").alias("type"),

        F.when(F.col("Episodes") == "UNKNOWN", 0.0)
        .otherwise(F.col("Episodes")).cast("double").alias("episodes"),

        F.col("Status").cast("string").alias("status"),
        F.col("Studios").alias("raw_studios"),
        F.col("Source").cast("string").alias("source"),
        F.col("Duration").cast("string").alias("duration"),
        F.col("Rating").cast("string").alias("rating"),

        # Using expr try_cast to handle empty or malformed strings
        F.expr("try_cast(Rank as double)").alias("rank"),
        F.expr("try_cast(Favorites as double)").alias("favorites"),
        F.col("Aired"),
        F.col("Premiered"))

    # 1. Create temporary columns that extract the first 4-digit number found
    working_df = working_df.withColumn("year_from_premiered",F.regexp_extract(F.col("Premiered"), r"(\d{4})", 1).cast("int")
    ).withColumn("year_from_aired",F.regexp_extract(F.col("Aired"), r"(\d{4})", 1).cast("int"))

    # 2. Use Coalesce to prioritize Premiered, then Aired, then default to 0 Coalesce picks the first NON-NULL value it sees
    working_df = working_df.withColumn("release_year",
        F.coalesce((F.when(F.col("year_from_premiered") > 0), F.col("year_from_premiered")),
                         F.when((F.col("year_from_aired") > 0), F.col("year_from_aired")),F.lit(0)))

    working_df = working_df.withColumn("release_year", F.expr("try_cast(regexp_extract(Aired, "
                                                              "'(\\d{4})', 1) as double)").cast("int"))
    working_df = working_df.withColumn("month_check", F.substring(F.col("Aired"), 1, 3))
    working_df = working_df.withColumn("season",
                                       F.when(F.col("month_check").isin("Apr", "May", "Jun"), "spring")
                                       .when(F.col("month_check").isin("Jul", "Aug", "Sep"), "summer")
                                       .when(F.col("month_check").isin("Oct", "Nov", "Dec"), "fall")
                                       .when(F.col("month_check").isin("Jan", "Feb", "Mar"), "winter")
                                       .otherwise("UNKNOWN"))

    working_df = working_df.withColumn("genres", F.split(F.col("raw_genres"), ", ")) \
        .withColumn("studios", F.split(F.col("raw_studios"), ", ")) \
        .withColumn("themes", F.array().cast("array<string>")) \
        .fillna(0.0, subset=["rank", "favorites", "episodes", "score"]) \
        .fillna(0, subset=["release_year"])
    return working_df.select(*MASTER_SCHEMA)

def integrate_datasets(j_df, a_df, k_df):

    print("\n[INFO] STARTING SMART INTEGRATION\n")

    # 1. Alias columns to keep track of sources 
    j = j_df.select(*(F.col(c).alias(f"j_{c}") for c in j_df.columns))
    a = a_df.select(*(F.col(c).alias(f"a_{c}") for c in a_df.columns))
    k = k_df.select(*(F.col(c).alias(f"k_{c}") for c in k_df.columns))

    # 2. Perform Full Outer Joins on mal_id
    combined = j.join(a, j.j_mal_id == a.a_mal_id, "full_outer").join(k, F.coalesce(F.col("j_mal_id"), F.col("a_mal_id")) == k.k_mal_id, "full_outer")

    # 3. Build the Master Record using COALESCE
    # Priority order: Jikan (j), then AniList (a), then Kaggle (k)
    final_df = combined.select(
        F.coalesce(F.col("j_mal_id"), F.col("a_mal_id"), F.col("k_mal_id")).alias("mal_id"),
        F.coalesce(F.col("j_title"), F.col("a_title"), F.col("k_title")).alias("title"),
        F.coalesce(F.col("j_title_english"), F.col("a_title_english"), F.col("k_title_english")).alias("title_english"),
        F.coalesce(F.col("j_release_year"), F.col("a_release_year"), F.col("k_release_year"), F.lit(0)).alias("release_year"),

        # 4. ENSEMBLE SCORE (The average of all non-null scores)
        F.expr("""
            (coalesce(j_score, 0) + coalesce(a_score, 0) + coalesce(k_score, 0)) / 
            (case when j_score is null or j_score = 0 then 0 else 1 end + 
             case when a_score is null or a_score = 0 then 0 else 1 end + 
             case when k_score is null or k_score = 0 then 0 else 1 end)
        """).alias("score"),

        F.coalesce(F.col("j_episodes"), F.col("a_episodes"), F.col("k_episodes"), F.lit(0)).alias("episodes"),
        F.coalesce(F.col("j_duration"), F.col("a_duration"), F.col("k_duration")).alias("duration"),
        F.coalesce(F.col("j_favorites"), F.col("a_favorites"), F.col("k_favorites"), F.lit(0)).alias("favorites"),
        F.coalesce(F.col("j_type"), F.col("a_type"), F.col("k_type")).alias("type"),
        F.coalesce(F.col("j_status"), F.col("a_status"), F.col("k_status")).alias("status"),
        F.coalesce(F.col("j_genres"), F.col("a_genres"), F.col("k_genres")).alias("genres"),
        F.coalesce(F.col("j_source"), F.col("a_source"), F.col("k_source")).alias("source"),
        F.coalesce(F.col("j_season"), F.col("a_season"), F.col("k_season")).alias("season"),
        F.coalesce(F.col("j_studios"), F.col("a_studios"), F.col("k_studios")).alias("studios"),
        F.coalesce(F.col("j_themes"), F.col("k_themes")).alias("themes"), # AniList lacks themes in our schema
        F.coalesce(F.col("j_rank"), F.col("k_rank")).alias("rank"),
        F.coalesce(F.col("j_rating"), F.col("k_rating")).alias("rating")
    )

    # 5. Clean up math edge cases (NaN scores) and force double types
    final_df = final_df.fillna(0.0, subset=["score", "rank", "favorites", "episodes"])
    final_df = final_df.fillna("Unknown", subset=["status", "type", "season", "rating"])

    print(f"[INFO]  Smart Integration Complete.")
    print(f"[INFO] Final Master Dataset count: {final_df.count()}")

    return final_df

def save_to_parquet(final_df):

    # 1. ENSURE DIRECTORY EXISTS
    processed_path = os.path.join(BASE_DIR, "data", "processed")
    if not os.path.exists(processed_path):
        os.makedirs(processed_path)
        print(f"[INFO] Created new directory: {processed_path}")

    # 2. SAVE THE FILE
    file_path = os.path.join(processed_path, "mega_anime.parquet")
    print(f"[INFO] Saving Final Dataset to Parquet: {file_path}")

    try:
        # Using 'overwrite' allows to re-run the pipeline safely
        final_df.write.mode("overwrite").parquet(file_path)
        print("[INFO] Parquet save successful.")
    except Exception as e:
        print(f"[ERROR] Failed to save Parquet: {e}")

