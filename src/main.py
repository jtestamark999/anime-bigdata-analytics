from datetime import datetime
from queries import *
from checking_health import *
from cleaning_data import *
from fetch_jikan import *
from fetch_anilist import *
from fetch_kaggle import *


def format_duration(seconds):
 mins = int(seconds // 60)
 secs = int(seconds % 60)
 return f"{mins}m {secs}s"

def run_pipeline(spark_input):
 start_time = time.time()
 print(f"\n[INFO] Starting pipeline run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

 # --- STEP 1: FETCHING ---
 # Assuming these functions return counts or dataframes
 print("[INFO] Fetching data from Jikan API...")
 fetch_jikan()

 print("[INFO] Fetching data from AniList...")
 fetch_anilist()

 print("[INFO] Fetching data from Kaggle...")
 fetch_kaggle()

 # --- STEP 2: CLEANING & TRANSFORMING ---
 print("[INFO] Applying transformations and cleaning data...")
 # This should save the final parquet to your processed_path

 raw_j_df = spark_input.read.option("multiLine", "true").json(jikan_folder)
 raw_a_df = spark_input.read.option("multiLine", "true").json(anilist_folder)
 raw_k_df = spark_input.read.format("csv").option("header", "true").option("multiLine", "true") \
  .option("quote", "\"").option("escape", "\"").load(kaggle_path)

 c_jikan = clean_jikan(raw_j_df)
 c_anilist = clean_anilist(raw_a_df)
 c_kaggle = clean_kaggle(raw_k_df)

 mega_df = integrate_datasets(c_jikan, c_anilist, c_kaggle)

 save_to_parquet(mega_df)
 # --- STEP 3: FINALIZING ---
 total_duration = format_duration(time.time() - start_time)
 print(f"[INFO] Pipeline complete. Duration: {total_duration}")
 return mega_df

if __name__ == "__main__":

 spark = SparkSession.builder.appName("AnimeProject").getOrCreate()
 # 1. SETUP PATHS
 BASE_DIR = os.path.dirname(os.path.abspath(__file__))
 processed_path = os.path.join(BASE_DIR, "data", "processed", "mega_anime.parquet")
 jikan_folder = os.path.join(BASE_DIR, "data/raw/jikan/")
 anilist_folder = os.path.join(BASE_DIR, "data/raw/anilist/")
 kaggle_path = os.path.join(BASE_DIR, "data/raw/anime-dataset-2023 2.csv")

# 1. Run the Data Engineering Pipeline
 run_pipeline(spark)
 try:
  df = spark.read.parquet(processed_path)
  df.createOrReplaceTempView("anime_data")
 except Exception as e:
  print(f"[ERROR] Could not load processed data: {e}")
  exit()

 # 2. Interactive Menu Loop
 while True:
  print("   ANIME BIG DATA TERMINAL")
  print("1. System Status")
  print("2. Quick Search")
  print("3. Top 10 Rankings")
  print("4. Industry Trends (Studios)")
  print("5. Surprise Me (Random Gem)")
  print("6. View Data Dictionary")
  print("7. View Data Health (All Sets)")
  print("0. Exit System")


  choice = input("\nSelect an option: ").strip()

  match choice:
   case "1":
    system_status(spark, processed_path)
   case "2":
    quick_search(spark, processed_path)
   case "3":
    get_rankings(spark, processed_path)
   case "4":
    industry_trends(spark, processed_path)
   case "5":
    surprise_me(spark,processed_path)
   case "6":
    display_data_dictionary()
   case "7":
    # To check health, we must READ the data into DataFrames first
    print("\n--- Health Report ---")
    check_data_health(spark.read.option("multiLine","true").json(jikan_folder), "Jikan Raw")
    check_data_health(spark.read.option("multiLine","true").json(anilist_folder), "Anilist Raw")
    raw_k_df = spark.read.format("csv") \
     .option("header", "true") \
     .option("multiLine", "true") \
     .option("quote", "\"") \
     .option("escape", "\"") \
     .load(kaggle_path)
    check_data_health(raw_k_df, "Kaggle Raw")
    check_data_health(spark.read.parquet(processed_path), "Final Master")
   case "0":
    print("\n[INFO] Shutting down Spark Session... Goodbye!")
    break
   case _:
    print("\n[!] Invalid selection. Please try again.")

 spark.stop()