import pandas as pd
import pyarrow.dataset as ds

SPARK = "/opt/data/output/spark_clean"
RAY = "/opt/data/output/ray_clean"
COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
        "PULocationID", "DOLocationID", "fare_amount", "total_amount",
        "pickup_borough", "pickup_zone", "speed_mph", "pickup_hour", "avg_speed_hour"]


def fingerprint(path):
    rows, total = 0, 0
    for batch in ds.dataset(path, format="parquet").to_batches(columns=COLS, batch_size=250_000):
        df = batch.to_pandas()
        for c in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
            df[c] = df[c].astype("datetime64[us]").astype("int64")
        for c in ["PULocationID", "DOLocationID", "pickup_hour"]:
            df[c] = df[c].astype("int64")
        df["avg_speed_hour"] = df["avg_speed_hour"].round(6)
        rows += len(df)
        total = (total + int(pd.util.hash_pandas_object(df, index=False).sum())) % 2**64
    return rows, total


def hourly(path):
    df = ds.dataset(path, format="parquet").to_table(columns=["pickup_hour", "avg_speed_hour"]).to_pandas()
    return df.drop_duplicates().set_index("pickup_hour").sort_index()["avg_speed_hour"]


spark_rows, spark_fp = fingerprint(SPARK)
print(f"Spark: rows={spark_rows} fingerprint={spark_fp}", flush=True)
ray_rows, ray_fp = fingerprint(RAY)
print(f"Ray:   rows={ray_rows} fingerprint={ray_fp}", flush=True)
print("MATCH" if (spark_rows, spark_fp) == (ray_rows, ray_fp) else "MISMATCH")

h = pd.DataFrame({"spark": hourly(SPARK), "ray": hourly(RAY)})
h["diff"] = (h["spark"] - h["ray"]).abs()
print(f"max hourly difference: {h['diff'].max():.2e}")