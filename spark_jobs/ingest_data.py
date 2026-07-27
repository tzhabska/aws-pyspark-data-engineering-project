from pyspark.sql import SparkSession
from spark_session import create_spark_session


INPUT_PATH = "data/raw/"
OUTPUT_PATH = "data/processed/"


def main():

    spark = create_spark_session()

    df = spark.read.csv(
        INPUT_PATH,
        header=True,
        inferSchema=True
    )

    df.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
