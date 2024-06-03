from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit
from pyspark.sql.functions import year
from google.cloud import storage

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ParquetGCSExample") \
    .getOrCreate()


source_bucket = "pipeline_test_pc"
source_prefix = "data_test/"
destination_bucket = "pipeline_test_pc"
destination_prefix = "data_test_upload/"

def list_files(bucket_name, prefix=""):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    files = [blob.name[10:] for blob in bucket.list_blobs(prefix=prefix) if blob.name.endswith('.parquet')]
    sorted_list = sorted(files, key=lambda x: (int(x[-15:-11]), int(x[-10:-8])))
    return files

files = list_files(source_bucket,source_prefix)

concatenated_df = None
sub_index = 1

for file_path in files:
    df = spark.read.parquet(f"gs://pipeline_test_pc/data_test/{file_path}")
    # df = df.withColumn("sub_index", lit(sub_index))
    # sub_index += 1  # Increment sub_index for the next file
    df = df.withColumn("year", year(df["dt_col"]))
    df = df.filter(df["year"] == 2023)
    df = df.drop('year')

    if concatenated_df is None:
        concatenated_df = df
    else:
        concatenated_df = concatenated_df.union(df)

concatenated_df = concatenated_df.coalesce(1)

concatenated_df = concatenated_df.withColumn("index", monotonically_increasing_id())
# window_spec = Window.orderBy("id")
# concatenated_df = concatenated_df.withColumn("index", row_number().over(window_spec) - 1)


#partitioned_df = concatenated_df.repartition("sub_index")
concatenated_df.write.mode('overwrite').parquet("gs://pipeline_test_pc/data_test_upload/")

# Stop SparkSession
spark.stop()