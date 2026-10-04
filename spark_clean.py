import glob
import os
import sys
import time
from functools import reduce
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType

INPUT = sys.argv[1]
ZONES = "/opt/spark-data/taxi_zone_lookup.csv"
OUTPUT = "/opt/spark-data/output/spark_clean"
SCHEMA = {
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "double",
    "trip_distance": "double",
    "PULocationID": "long",
    "DOLocationID": "long",
    "fare_amount": "double",
    "total_amount": "double",
}

spark = (SparkSession.builder
         .appName("spark_clean")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())


def speed_mph(distance, pickup, dropoff):
    hours = (dropoff - pickup).total_seconds() / 3600
    return distance / hours


speed_udf = F.udf(speed_mph, DoubleType())

start = time.time()

# Ingestion
files = sorted(glob.glob(os.path.join(INPUT, "*.parquet"))) if os.path.isdir(INPUT) else [INPUT]
parts = [spark.read.parquet(f).select([F.col(c).cast(t) for c, t in SCHEMA.items()]) for f in files]
trips = reduce(DataFrame.unionByName, parts)
zones = spark.read.csv(ZONES, header=True, inferSchema=True)

# Cleansing
trips = (trips.dropna()
         .dropDuplicates()
         .withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
         .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
         .filter((F.col("trip_distance") > 0) &
                 (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))))

# join: attaching pickup borough and zone name
zones = zones.select(F.col("LocationID").alias("PULocationID"),
                     F.col("Borough").alias("pickup_borough"),
                     F.col("Zone").alias("pickup_zone"))
trips = trips.join(zones, on="PULocationID", how="inner")

# features: trip speed, avg speed per hour
trips = (trips.withColumn("speed_mph", speed_udf("trip_distance", "tpep_pickup_datetime", "tpep_dropoff_datetime"))
         .filter(F.col("speed_mph") <= 100)
         .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
         .cache())
trips.count()
hourly = trips.groupBy("pickup_hour").agg(F.avg("speed_mph").alias("avg_speed_hour"))
result = trips.join(hourly, on="pickup_hour")

# export
result.write.mode("overwrite").parquet(OUTPUT)
total = time.time() - start

# same speed formula: python UDF vs native Spark
out = spark.read.parquet(OUTPUT).cache()
rows = out.count()

t = time.time()
out.select(F.sum(speed_udf("trip_distance", "tpep_pickup_datetime", "tpep_dropoff_datetime"))).collect()
udf_time = time.time() - t

native = F.col("trip_distance") / (
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600)
t = time.time()
out.select(F.sum(native)).collect()
native_time = time.time() - t

print(f"ROWS={rows} TOTAL={total:.2f}s UDF={udf_time:.2f}s NATIVE={native_time:.2f}s")
input("Spark UI is live on localhost:4040. Press Enter to exit...")
spark.stop()