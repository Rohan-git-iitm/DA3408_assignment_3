import glob
import os
import shutil
import sys
import time
import pandas as pd
import ray
from ray.data.context import DataContext, ShuffleStrategy

INPUT = sys.argv[1]
ZONES = "/opt/data/taxi_zone_lookup.csv"
OUTPUT = "/opt/data/output/ray_clean"
TMP = "/opt/data/output/ray_tmp"
SCHEMA = {
    "tpep_pickup_datetime": "datetime64[us]",
    "tpep_dropoff_datetime": "datetime64[us]",
    "passenger_count": "float64",
    "trip_distance": "float64",
    "PULocationID": "int64",
    "DOLocationID": "int64",
    "fare_amount": "float64",
    "total_amount": "float64",
}

ray.init(address="auto")
DataContext.get_current().shuffle_strategy = ShuffleStrategy.SORT_SHUFFLE_PULL_BASED
shutil.rmtree(OUTPUT, ignore_errors=True)
shutil.rmtree(TMP, ignore_errors=True)


def speed_mph(distance, pickup, dropoff):
    hours = (dropoff - pickup).total_seconds() / 3600
    return distance / hours


def clean_batch(df):
    df = df[list(SCHEMA)].dropna().astype(SCHEMA)
    df = df[(df["trip_distance"] > 0) &
            (df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"])].reset_index(drop=True)
    # identical rows get identical hashes, so duplicates always share a bucket
    df["bucket"] = pd.util.hash_pandas_object(df, index=False) % 8
    return df


def drop_dups(df):
    return df.drop_duplicates(subset=list(SCHEMA)).drop(columns="bucket").reset_index(drop=True)


def merge_with(df, small, key):
    return df.merge(small, on=key, how="inner")


def add_speed(df):
    df["speed_mph"] = [speed_mph(d, p, o) for d, p, o in
                       zip(df["trip_distance"], df["tpep_pickup_datetime"], df["tpep_dropoff_datetime"])]
    df = df[df["speed_mph"] <= 100]
    df = df.assign(pickup_hour=df["tpep_pickup_datetime"].dt.hour)
    return df.reset_index(drop=True)


start = time.time()

# ingestion and cleansing
files = sorted(glob.glob(os.path.join(INPUT, "*.parquet"))) if os.path.isdir(INPUT) else [INPUT]
parts = [ray.data.read_parquet(f).select_columns(list(SCHEMA))
         .map_batches(clean_batch, batch_format="pandas", batch_size=None) for f in files]
trips = parts[0].union(*parts[1:]) if len(parts) > 1 else parts[0]
zones = pd.read_csv(ZONES, keep_default_na=False, na_values=[""])

# removing duplicate rows: shuffle by bucket, then drop duplicates within each bucket
trips = trips.groupby("bucket", num_partitions=8).map_groups(drop_dups, batch_format="pandas")

# join: attaching pickup borough and zone name
zones = zones.rename(columns={"LocationID": "PULocationID", "Borough": "pickup_borough",
                              "Zone": "pickup_zone"})[["PULocationID", "pickup_borough", "pickup_zone"]]
trips = trips.map_batches(merge_with, fn_kwargs={"small": zones, "key": "PULocationID"},
                          batch_format="pandas", batch_size=None)

# features: trip speed, avg speed per hour (checkpoint to Parquet instead of holding it in memory)
trips.map_batches(add_speed, batch_format="pandas", batch_size=None).write_parquet(TMP)
trips = ray.data.read_parquet(TMP)
hourly = trips.groupby("pickup_hour", num_partitions=8).mean("speed_mph").to_pandas()
hourly = hourly.rename(columns={"mean(speed_mph)": "avg_speed_hour"})
result = trips.map_batches(merge_with, fn_kwargs={"small": hourly, "key": "pickup_hour"},
                           batch_format="pandas", batch_size=None)

# export
result.write_parquet(OUTPUT)
total = time.time() - start


# speed formula: python UDF vs pandas
def udf_batch(df):
    s = sum(speed_mph(d, p, o) for d, p, o in
            zip(df["trip_distance"], df["tpep_pickup_datetime"], df["tpep_dropoff_datetime"]))
    return pd.DataFrame({"s": [s]})


def vec_batch(df):
    hours = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 3600
    return pd.DataFrame({"s": [(df["trip_distance"] / hours).sum()]})


out = ray.data.read_parquet(OUTPUT).materialize()
rows = out.count()

t = time.time()
out.map_batches(udf_batch, batch_format="pandas", batch_size=None).to_pandas()["s"].sum()
udf_time = time.time() - t

t = time.time()
out.map_batches(vec_batch, batch_format="pandas", batch_size=None).to_pandas()["s"].sum()
native_time = time.time() - t

print(f"ROWS={rows} TOTAL={total:.2f}s UDF={udf_time:.2f}s NATIVE={native_time:.2f}s")