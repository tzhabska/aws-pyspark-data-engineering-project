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


# Read raw data
df = spark.read.parquet(raw_path)


print("Input count:")
print(df.count())


# Get available year/month combinations
months = (
    df
    .select(
        F.year("tpep_pickup_datetime").alias("year"),
        F.month("tpep_pickup_datetime").alias("month")
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
        df
        .filter(
            (F.year("tpep_pickup_datetime") == year) &
            (F.month("tpep_pickup_datetime") == month)
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