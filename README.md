# DA3408 Assignment 3: Spark vs. Ray

Identical data preprocessing pipeline in Apache Spark and Ray Data, benchmarked on two-worker Docker clusters.

**Author:** Rohan Sai P. (DA24B049)

## Results (six months of NYC Yellow Taxi data, 18,717,045 output rows)

| | Spark 3.3.4 | Ray 2.59.0 |
|---|---|---|
| Total time (ingestion to export) | 281.5 s | 141.6 s |
| Python UDF (row by row) | 50.8 s | 50.3 s |
| Native / vectorized | 2.4 s | 6.0 s |
| Cluster peak CPU | 368% | 372% |
| Cluster peak memory | 4,535 MiB | 5,907 MiB |

Both outputs are identical (matching row-level hash checksums). Full analysis in `report.pdf`.

## Pipeline

1. Ingest six monthly Parquet files with one fixed schema
2. Clean: drop nulls and duplicates, cast timestamps, filter invalid trips
3. Join the taxi zone lookup; compute per-trip speed with a Python UDF; add average speed per pickup hour
4. Export to Parquet

## Repository layout

| Path | Contents |
|---|---|
| `spark_clean.py` | Spark pipeline |
| `ray_clean.py` | Ray Data pipeline |
| `spark/` | Spark image (Dockerfile, start script) and cluster (docker-compose.yml): master + 2 workers |
| `ray/` | Ray image and cluster: head + 2 workers |
| `scripts/parity.py` | Checks the two outputs match row for row |
| `scripts/monitor.sh`, `scripts/peaks.py` | Record and summarise per-container CPU and memory |
| `results/` | Raw `docker stats` recordings of the benchmark runs |
| `report.pdf` | Benchmark report |

## Running

Data (not included): download `yellow_tripdata_2023-01` to `-06.parquet` and `taxi_zone_lookup.csv` from the
[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) page into `data/taxi/` and `data/`.

Spark (put `apache-spark.tgz` for Spark 3.3.4 next to the Dockerfile first):

```bash
cd spark && docker compose build && docker compose up -d spark-master spark-worker-a spark-worker-b
docker exec -it spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --executor-memory 1500m /opt/spark-apps/spark_clean.py /opt/spark-data/taxi
```

Ray:

```bash
cd ray && docker compose build && docker compose up -d
docker exec ray_head python /opt/ray-apps/ray_clean.py /opt/data/taxi
```

Run one cluster at a time. UIs: Spark master `localhost:9090`, Spark job `localhost:4040`, Ray dashboard `localhost:8265`.
