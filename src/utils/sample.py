import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_sample(spark):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = script_dir

    input_path = os.path.join(project_root, "data", "processed", "mega_anime.parquet")
    output_path = os.path.join(project_root, "data", "sample", "anime_sample")

    print(f"Attempting to read from: {input_path}")

    if not os.path.exists(input_path):
        print(f"ERROR: Path not found: {input_path}")
        return

    df = spark.read.parquet(input_path)
    for col_name, col_type in df.dtypes:
        if "array" in col_type:
            print(f"Flattening column: {col_name}")
            df = df.withColumn(col_name, F.concat_ws(", ", F.col(col_name)))

    df.limit(100).coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"Sample successfully saved to: {output_path}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SampleGenerator").getOrCreate()
    create_sample(spark)