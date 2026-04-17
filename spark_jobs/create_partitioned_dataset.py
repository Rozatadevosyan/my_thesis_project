from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CreatePartitionedDataset")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true")
    .getOrCreate()
)

print("Spark Session Started")

visits = spark.read.parquet("hdfs://namenode:9000/medical_data/patient_visits")
print("Non-partitioned dataset loaded")

visits.write \
    .mode("overwrite") \
    .partitionBy("diagnosis") \
    .option("dfs.replication", "1") \
    .parquet("hdfs://namenode:9000/medical_data/patient_visits_partitioned")

print("Partitioned dataset created successfully")

spark.stop()
print("Done")