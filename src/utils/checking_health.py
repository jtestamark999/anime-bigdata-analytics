from pyspark.sql import functions as F

def check_data_health(df, dataset_name):
    print(f"\n Health Report: {dataset_name} ")
    health_checks = []
    for c, dtype in df.dtypes:
        # Handle specific nested objects and check if null or empty array
        if c in ["studios", "producers", "genres"]:
            if "struct" in dtype:
                # If it's a struct with a nodes or data array
                inner_field = "nodes" if "nodes" in dtype else "data" if "data" in dtype else None
                if inner_field:
                    check = F.count(F.when(F.col(c).isNull() | (F.size(F.col(f"{c}.{inner_field}")) == 0), c)).alias(c)
                else:
                    check = F.count(F.when(F.col(c).isNull(), c)).alias(c)
            elif "array" in dtype:
                check = F.count(F.when(F.col(c).isNull() | (F.size(F.col(c)) == 0), c)).alias(c)
            else:
                check = F.count(F.when(F.col(c).isNull(), c)).alias(c)

        # General Structs
        elif "struct" in dtype:
            check = F.count(F.when(F.col(c).isNull(), c)).alias(c)

        # Standard types Strings, Longs, CSV columns
        else:
            check = F.count(F.when(
                F.col(c).isNull() |
                (F.col(c).cast("string") == "Unknown") |
                (F.col(c).cast("string") == "") |
                (F.col(c).cast("string") == "N/A")
                , c)).alias(c)
        health_checks.append(check)
    df.select(health_checks).show()