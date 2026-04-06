from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, size

def check_data_health(df, dataset_name):

    print(f"\n Health Report: {dataset_name} ")

    health_checks = []
    for c, dtype in df.dtypes:
        # 1. Handle Jikan/AniList specific nested 'studios' or 'producers'
        #check if the struct itself is null or the internal array is empty
        if c in ["studios", "producers", "genres"]:
            if "struct" in dtype:
                # If it's a struct with a 'nodes' or 'data' array
                inner_field = "nodes" if "nodes" in dtype else "data" if "data" in dtype else None
                if inner_field:
                    check = count(when(col(c).isNull() | (size(col(f"{c}.{inner_field}")) == 0), c)).alias(c)
                else:
                    check = count(when(col(c).isNull(), c)).alias(c)
            elif "array" in dtype:
                check = count(when(col(c).isNull() | (size(col(c)) == 0), c)).alias(c)
            else:
                check = count(when(col(c).isNull(), c)).alias(c)

        # 2. General Structs
        elif "struct" in dtype:
            check = count(when(col(c).isNull(), c)).alias(c)

        # 3. Standard types (Strings, Longs, CSV columns)
        else:
            check = count(when(
                col(c).isNull() |
                (col(c).cast("string") == "Unknown") |
                (col(c).cast("string") == "") |
                (col(c).cast("string") == "N/A")
                , c)).alias(c)
        health_checks.append(check)
    df.select(health_checks).show()

def run_pipeline():
    spark = SparkSession.builder.appName("AnimeDataCleaning") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1").getOrCreate()

    # Define  paths
    base_path = "/Users/tempest/Documents/Code/CS 4265 Anime Project/data/raw"
    paths = {
        "AniList": f"{base_path}/anilist/*.json",
        "Jikan": f"{base_path}/jikan/*.json",
        "Kaggle CSV": f"{base_path}/anime-dataset-2023 2.csv"

    }
    print(" Starting Multi-Source Spark Pipeline ")
    for name, path in paths.items():
        try:
            if "csv" in path:
                df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
            else:
                df = spark.read.option("multiLine", "true").json(path)

            print(f"\n {name}: Loaded {df.count()} records.")
            check_data_health(df, name)
        except Exception as e:
            print(f"️ Could not process {name}: {str(e)}")

    print("\nAll Health Checks Finished")
    spark.stop()

