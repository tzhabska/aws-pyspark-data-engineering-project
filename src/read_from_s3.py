from pyspark.sql import SparkSession

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
    .getOrCreate()

df = spark.read.parquet(
    "s3a://aws-pyspark-taxi-project-tetiana/raw/"
)

df.show(5)
print(df.count())
df.printSchema()