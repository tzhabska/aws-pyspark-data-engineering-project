from pyspark.sql import SparkSession


def create_spark_session():
    """
    Creates and returns Spark session for the data pipeline.
    """

    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_Data_Pipeline")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    return spark
