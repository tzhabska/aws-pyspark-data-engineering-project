import boto3

s3 = boto3.client("s3")

bucket = "aws-pyspark-taxi-project-tetiana"

files = [
    "yellow_tripdata_2024-01.parquet",
    "yellow_tripdata_2024-02.parquet",
    "yellow_tripdata_2024-03.parquet"
]

for file in files:
    s3.upload_file(
        f"data/raw/{file}",
        bucket,
        f"raw/{file}"
    )

print("Upload completed")