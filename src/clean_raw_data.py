from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder \
    .appName("NYC_Taxi_Data_Pipeline") \
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.5.0,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ) \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.profile.ProfileCredentialsProvider"
    ) \
    .config(
        "spark.sql.shuffle.partitions",
        "8"
    ) \
    .config(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic"
    ) \
    .getOrCreate()


raw_path = "s3a://aws-pyspark-taxi-project-tetiana/raw/"
processed_path = "s3a://aws-pyspark-taxi-project-tetiana/processed/"


df_raw_2024 = spark.read.parquet(raw_path)
print("Input count:")
print(df_raw_2024.count())



df_2024_clean = (
    df_raw_2024
    .withColumn(
        "pickup_date",
        F.to_date("tpep_pickup_datetime")
    )
    .filter(
        (F.year("pickup_date") == 2024) &
        (F.month("pickup_date").isin(1, 2, 3))
    )
    .withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") -
         F.unix_timestamp("tpep_pickup_datetime")) / 60
    )
    .withColumn(
        "avg_speed_mph",
        F.when(
            F.col("trip_duration_minutes") > 0,
            F.col("trip_distance") /
            (F.col("trip_duration_minutes") / 60)
        )
    )
    .withColumn(
        "passenger_count_missing",
        F.when(
            F.col("passenger_count").isNull(), 1
        ).otherwise(0)
    )
    .withColumn(
        "congestion_surcharge_missing",
        F.when(
            F.col("congestion_surcharge").isNull(), 1
        ).otherwise(0)
    )
    .withColumn(
        "Airport_fee_missing",
        F.when(
            F.col("Airport_fee").isNull() &
            F.col("PULocationID").isin(132, 1, 138),
            1
        ).otherwise(0)
    )
    .withColumn(
        "negative_amount_flag",
        F.when(
            (F.col("fare_amount") < 0) |
            (F.col("total_amount") < 0) |
            (F.col("tip_amount") < 0) |
            (F.col("tolls_amount") < 0),
            1
        ).otherwise(0)
    )
    .withColumn(
        "long_distance_flag",
        F.when(
            (F.col("trip_distance") > 100) &
            (F.col("avg_speed_mph") > 85),
            1
        ).otherwise(0)
    )
)



print("Clean count:")
print(df_2024_clean.count())

df_2024_clean.select(
    F.min("pickup_date").alias("min_date"),
    F.max("pickup_date").alias("max_date")
).show()

df_2024_clean.printSchema()

df_2024_clean.select(
    F.sum("passenger_count_missing").alias("passenger_count_missing"),
    F.sum("congestion_surcharge_missing").alias("congestion_surcharge_missing"),
    F.sum("Airport_fee_missing").alias("Airport_fee_missing"),
    F.sum("negative_amount_flag").alias("negative_amount_flag"),
    F.sum("long_distance_flag").alias("long_distance_flag")
).show()

# Get available year/month combinations
months = (
    df_2024_clean
    .select(
        F.year("pickup_date").alias("year"),
        F.month("pickup_date").alias("month")
    )
    .distinct()
    .orderBy("year", "month")
    .collect()
)


print("Partitions to process:")
for row in months:
    print(f"year={row['year']}, month={row['month']}")


# Process and write one year/month partition at a time
for row in months:

    year = row["year"]
    month = row["month"]

    print(f"\nProcessing year={year}, month={month}...")

    df_month = (
        df_2024_clean
        .filter(
            (F.year("pickup_date") == year) &
            (F.month("pickup_date") == month)
        )
        .withColumn("year", F.lit(year))
        .withColumn("month", F.lit(month))
    )

    print("Rows in partition:")
    print(df_month.count())

    (
        df_month
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(processed_path)
    )

    print(f"Finished year={year}, month={month}")


print("\nProcessed data written successfully")


spark.stop()


