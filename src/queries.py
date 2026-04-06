import os

from pyspark.sql import functions as F
from pyspark.sql import types as T

# Define the schema globally so all functions can use it
schema = T.StructType([
    T.StructField("mal_id", T.DoubleType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("title_english", T.StringType(), True),
    T.StructField("release_year", T.IntegerType(), True),
    T.StructField("score", T.DoubleType(), True),
    T.StructField("episodes", T.DoubleType(), True),
    T.StructField("duration", T.StringType(), True),
    T.StructField("favorites", T.DoubleType(), True),
    T.StructField("type", T.StringType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("genres", T.ArrayType(T.StringType()), True),
    T.StructField("source", T.StringType(), True),
    T.StructField("season", T.StringType(), True),
    T.StructField("studios", T.ArrayType(T.StringType()), True),
    T.StructField("themes", T.ArrayType(T.StringType()), True),
    T.StructField("rank", T.DoubleType(), True),
    T.StructField("rating", T.StringType(), True)
])

def quick_search(spark, parquet_path):
    print("\n QUICK SEARCH ")
    raw_query = input("Enter Anime Name (e.g., One Piece): ").strip()
    if not raw_query:
        return

    try:
        # Load the analytical dataset
        df = spark.read.parquet(parquet_path)
        df.createOrReplaceTempView("search_table")

        #  SMART SEARCH LOGIC 
        # 1. We lowercase the query
        # 2. We remove all spaces 
        clean_query = raw_query.lower().replace(" ", "")
        search_term = f"%{clean_query}%"

        
        # this handles cases where the DB says "Dragon Ball" but user types "DragonBall"
        results = spark.sql(f"""
            SELECT * FROM search_table 
            WHERE lower(replace(title, ' ', '')) LIKE '{search_term}' 
            OR lower(replace(title_english, ' ', '')) LIKE '{search_term}'
            ORDER BY release_year DESC
        """)

        data = results.collect()

        if data:
            print(f"\n Found {len(data)} match(es)! Displaying all results:")
            for i, row in enumerate(data, 1):
                print(f"\n--- [RESULT #{i}] ---")
                print(f"MAL ID        | {int(row['mal_id']) if row['mal_id'] else 'N/A'}")
                print(f"TITLE         | {row['title']}")
                print(f"ENGLISH       | {row['title_english'] or 'N/A'}")
                print(f"YEAR          | {row['release_year'] or 'Unknown'}")
                print(f"SCORE         | {row['score']:.2f}/10" if row['score'] else "SCORE         | N/A")
                print(f"EPISODES      | {int(row['episodes']) if row['episodes'] else 0}")
                print(f"DURATION      | {row['duration'] or 'Unknown'}")
                print(f"FAVORITES     | {int(row['favorites']) if row['favorites'] else 0}")
                print(f"FORMAT        | {row['type'] or 'N/A'}")
                print(f"STATUS        | {row['status'] or 'N/A'}")

                # Handling lists
                genres = ", ".join(row['genres']) if row['genres'] else "N/A"
                studios = ", ".join(row['studios']) if row['studios'] else "Unknown"
                themes = ", ".join(row['themes']) if row['themes'] else "None Listed"

                print(f"GENRES        | {genres}")
                print(f"SOURCE        | {row['source'] or 'Original'}")
                print(f"SEASON        | {row['season'] or 'Unknown'}")
                print(f"STUDIOS       | {studios}")
                print(f"THEMES        | {themes}")

                rank_val = f"#{int(row['rank'])}" if row['rank'] and row['rank'] > 0 else "N/A"
                print(f"RANK          | {rank_val}")
                print(f"RATING        | {row['rating'] or 'Not Rated'}")
                print("-" * 30)

            print(f"\n End of results. Total found: {len(data)}")
        else:
            print(f"\nNo anime found matching '{raw_query}'. Try a different keyword!")

    except Exception as e:
        print(f"\n[SYSTEM ERROR] Could not locate Parquet at {parquet_path}: {e}")
        return

def get_rankings(spark, parquet_path):
    print("\n --- TOP 10 RANKINGS ---")

    try:
        # Load the analytical dataset
        df = spark.read.parquet(parquet_path)
    except Exception as e:
        print(f"\n[SYSTEM ERROR] Could not locate Parquet at {parquet_path}: {e}")
        return


    year_input = input("Enter Year (e.g., 2024, or press Enter to skip): ").strip()
    genre_input = input("Enter Genre (e.g., Action, or press Enter to skip): ").strip().capitalize()

    # Filtering Logic 
    filtered_df = df
    if year_input:
        try:
            filtered_df = filtered_df.filter(F.col("release_year") == int(year_input))
        except ValueError:
            print(f" Invalid year '{year_input}'. Skipping year filter.")

    if genre_input:
        filtered_df = filtered_df.filter(F.array_contains(F.col("genres"), genre_input))

    #  Sorting & Collecting
    # Sort by Score descending and take Top 10
    results = filtered_df.orderBy(F.col("score").desc()).limit(10)
    data = results.collect()

    # Full Detail Printing 
    if data:
        print(f"\n Showing Top 10 {genre_input or 'All Genres'} Anime from {year_input or 'All Time'}:")
        for i, row in enumerate(data, 1):
            print(f"\n--- [RANK #{i}] ---")
            print(f"MAL ID        | {int(row['mal_id']) if row['mal_id'] else 'N/A'}")
            print(f"TITLE         | {row['title']}")
            print(f"ENGLISH       | {row['title_english'] or 'N/A'}")
            print(f"YEAR          | {row['release_year'] or 'Unknown'}")
            print(f"SCORE         | {row['score']:.2f}/10" if row['score'] else "SCORE         | N/A")
            print(f"EPISODES      | {int(row['episodes']) if row['episodes'] else 0}")
            print(f"DURATION      | {row['duration'] or 'Unknown'}")
            print(f"FAVORITES     | {int(row['favorites']) if row['favorites'] else 0}")
            print(f"FORMAT        | {row['type'] or 'N/A'}")
            print(f"STATUS        | {row['status'] or 'N/A'}")

            # Handling Array Columns
            genres_list = ", ".join(row['genres']) if row['genres'] else "N/A"
            studios_list = ", ".join(row['studios']) if row['studios'] else "Unknown"
            themes_list = ", ".join(row['themes']) if row['themes'] else "None Listed"

            print(f"GENRES        | {genres_list}")
            print(f"SOURCE        | {row['source'] or 'Original'}")
            print(f"SEASON        | {row['season'] or 'Unknown'}")
            print(f"STUDIOS       | {studios_list}")
            print(f"THEMES        | {themes_list}")

            # Ranking and Rating
            rank_val = f"#{int(row['rank'])}" if row['rank'] and row['rank'] > 0 else "N/A"
            print(f"RANK          | {rank_val}")
            print(f"RATING        | {row['rating'] or 'Not Rated'}")
            print("-" * 30)
    else:
        print(f"\n No results found for Year: {year_input or 'Any'} | Genre: {genre_input or 'Any'}")

def industry_trends(spark, parquet_path):
    print("\n--- STUDIO PERFORMANCE ANALYSIS ---")

    try:
        # 1. Load the analytical dataset
        df = spark.read.parquet(parquet_path)
    except Exception as e:
        print(f"\n[SYSTEM ERROR] Could not read Parquet file: {e}")
        return

    # 2. Processing: Explode studios, aggregate, and filter
    # Anime can have multiple studios
    trends = df.withColumn("studio", F.explode(F.col("studios"))) \
        .groupBy("studio") \
        .agg(
        F.avg("score").alias("Avg_Score"),
        F.count("mal_id").alias("Total_Anime"),
        F.max("score").alias("Top_Score")
    ) \
        .filter("Total_Anime > 10") \
        .orderBy(F.col("Avg_Score").desc()) \
        .limit(10)

    # 3. Fetch data to local memory
    data = trends.collect()

    # 4. Print results 
    if data:
        print(f"\n[INSIGHT] Top 10 Studios by Average Rating (Minimum 10 Shows):")
        for i, row in enumerate(data, 1):
            print(f"\n--- [STUDIO RANK #{i}] ---")
            print(f"STUDIO NAME   | {row['studio']}")
            print(f"AVG RATING    | {row['Avg_Score']:.2f}/10")
            print(f"TOTAL ANIME   | {int(row['Total_Anime'])}")
            print(f"HIGHEST SCORE | {row['Top_Score']:.2f}/10" if row['Top_Score'] else "HIGHEST SCORE | N/A")

            # Progress bar visual 
            bar_length = int(row['Avg_Score'] * 2)
            print(f"QUALITY BAR   | {'★' * bar_length}")
            print("-" * 30)
    else:
        print("\n No studios found with more than 10 anime records.")

def surprise_me(spark, parquet_path):
    print("\n --- SURPRISE ME: RANDOM HIDDEN GEM ---")
    genre_input = input("Enter a Genre you love (e.g., Sci-Fi): ").strip().capitalize()

    try:
        # 2. Load the analytical dataset
        df = spark.read.parquet(parquet_path)

        # 3. Filter for High Quality (Score > 8.0)
        
        hidden_gems = df.filter(
            (F.array_contains(F.col("genres"), genre_input)) &
            (F.col("score") >= 8.0)
        )

        # Pick exactly 1 random row
        random_pick = hidden_gems.orderBy(F.rand()).limit(1).collect()

        # 4. Display in the Quick Search
        if random_pick:
            row = random_pick[0]
            print(f"\n [SURPRISE RECOMMENDATION]")
            print(f"--- [RANDOM PICK] ---")
            print(f"MAL ID        | {int(row['mal_id']) if row['mal_id'] else 'N/A'}")
            print(f"TITLE         | {row['title']}")
            print(f"ENGLISH       | {row['title_english'] or 'N/A'}")
            print(f"YEAR          | {row['release_year'] or 'Unknown'}")
            print(f"SCORE         | {row['score']:.2f}/10" if row['score'] else "SCORE         | N/A")
            print(f"EPISODES      | {int(row['episodes']) if row['episodes'] else 0}")
            print(f"DURATION      | {row['duration'] or 'Unknown'}")
            print(f"FAVORITES     | {int(row['favorites']) if row['favorites'] else 0}")
            print(f"FORMAT        | {row['type'] or 'N/A'}")
            print(f"STATUS        | {row['status'] or 'N/A'}")

            # Handle Array Columns
            genres_list = ", ".join(row['genres']) if row['genres'] else "N/A"
            studios_list = ", ".join(row['studios']) if row['studios'] else "Unknown"
            themes_list = ", ".join(row['themes']) if row['themes'] else "None Listed"

            print(f"GENRES        | {genres_list}")
            print(f"SOURCE        | {row['source'] or 'Original'}")
            print(f"SEASON        | {row['season'] or 'Unknown'}")
            print(f"STUDIOS       | {studios_list}")
            print(f"THEMES        | {themes_list}")

            # Ranking and Rating
            rank_val = f"#{int(row['rank'])}" if row['rank'] and row['rank'] > 0 else "N/A"
            print(f"RANK          | {rank_val}")
            print(f"RATING        | {row['rating'] or 'Not Rated'}")
            print("-" * 30)
            print(" Pro Tip: If you don't like this one, run 'Surprise Me' again!")

        else:
            print(f"\n[!] No high-rated (8.0+) matches found for '{genre_input}'. Try a broader genre like 'Action' or 'Drama'!")

    except Exception as e:
        print(f"\n[SYSTEM ERROR] {e}")

def system_status(spark, parquet_path):
    print("\n --- SYSTEM & DATASET STATUS ---")

    try:
        # Load the analytical dataset locally for this function
        df = spark.read.parquet(parquet_path)
        df.createOrReplaceTempView("status_table")

        # Perform aggregation via Spark SQL
        stats = spark.sql("""
                          SELECT
                              COUNT(*) as total_records,
                              AVG(score) as avg_score,
                              MIN(release_year) as oldest_year,
                              MAX(release_year) as newest_year
                          FROM status_table
                          """).collect()[0]

        print(f"DATABASE TYPE | Spark SQL (Parquet-backed)")
        print(f"STORAGE PATH  | {os.path.basename(parquet_path)}")
        print(f"TOTAL RECORDS | {stats['total_records']:,} anime entries")
        print(f"AVG RATING    | {stats['avg_score']:.2f}/10" if stats['avg_score'] else "AVG RATING    | N/A")
        print(f"YEAR RANGE    | {int(stats['oldest_year'])} - {int(stats['newest_year'])}")
        print(f"SYSTEM STATUS | [ONLINE - OPTIMIZED]")
        print("-" * 30)

    except Exception as e:
        print(f"\n[SYSTEM ERROR] Could not locate Parquet at {parquet_path}: {e}")
        return

def display_data_dictionary():
    print("\n" + "="*50)
    print(" --- DATA DICTIONARY: MEGA ANIME DATASET ---")
    print("="*50)
    # Define the dictionary structure
    dictionary = [
        ("mal_id", "Integer", "Unique ID from MyAnimeList (Primary Key)"),
        ("title", "String", "Primary Japanese/Romaji title"),
        ("title_english", "String", "Official English title"),
        ("release_year", "Integer", "Year the anime first aired"),
        ("score", "Double", "Average rating (0.00 to 10.00)"),
        ("episodes", "Integer", "Total episodes produced"),
        ("duration", "String", "Length per episode in minuets  (e.g., '24 min')"),
        ("favorites", "Integer", "Total users who favorited the entry"),
        ("type", "String", "Format (TV, Movie, OVA, etc.)"),
        ("status", "String", "Airing status (Finished, Current)"),
        ("genres", "Array", "List of genres (Action, Sci-Fi, etc.)"),
        ("source", "String", "Original material (Manga, Novel)"),
        ("season", "String", "Airing season (Spring, Fall, etc.)"),
        ("studios", "Array", "Production companies"),
        ("themes", "Array", "Narrative themes (Mecha, Isekai)"),
        ("rank", "Integer", "Global popularity/score rank"),
        ("rating", "String", "Age classification (PG-13, R, etc.)")
    ]

    print(f"{'FIELD NAME':<16} | {'TYPE':<10} | {'DESCRIPTION'}")
    print("-" * 75)
    for field, dtype, desc in dictionary:
        print(f"{field:<16} | {dtype:<10} | {desc}")
    print("="*75)
