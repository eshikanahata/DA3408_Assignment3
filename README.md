# DA3408 Assignment 3 

Author: Eshika Nahata (DA24B004)

The same NYC taxi cleaning pipeline is implemented in Apache Spark (PySpark) and Ray Data. Both run on a two-worker cluster, and they are compared on execution time, resource usage and Python-UDF overhead.

## Results (single run per framework)

| Metric | Spark | Ray |
|---|---|---|
| Raw rows in | 87,595,259 | 87,595,259 |
| Rows out | 87,333,714 | 87,333,714 |
| Wall-clock, ingestion to export (s) | 220.19 | 1816.4 |
| Peak CPU, system-wide (%) | 86.8 | 67.2 |
| Average CPU, system-wide (%) | 39.6 | 12.2 |
| Peak memory used (GB) | 13.17 | 11.97 |
| Peak memory above idle baseline (GB) | 8.75 | 7.26 |

UDF micro-benchmark (87,333,714 rows):

| Engine | Variant | Time (s) |
|---|---|---|
| Spark | baseline (trivial JVM op) | 1.78 |
| Spark | native JVM expression | 7.23 |
| Spark | pandas_udf (Arrow) | 13.70 (+6.47 vs native JVM) |
| Ray | identity `map_batches` | 4.29 |
| Ray | Python function `map_batches` | 5.13 (+0.84 vs identity) |

Parity check (`verify_parity.py`): **PASS**. Both outputs have 87,333,714 rows, an identical row-hash sum (-47985407622463961372187) and an identical `sum(speed_mph)` (1136114430.0).

Spark was 8.25x faster end to end. See the report for the discussion and the AI-first vs BI-first recommendation.

## Repository layout

| File | Purpose |
|---|---|
| `common.py` | Shared schema mapping, constants, speed function, argument parsing, Spark session builder |
| `spark_clean.py` | PySpark pipeline |
| `ray_clean.py` | Ray Data pipeline (runs in hash-bucket passes, default 4) |
| `ray_cluster.py` | Starts a local Ray cluster: head with 0 CPUs plus 2 worker nodes |

## Pipeline

1. **Ingest:** read 4 FHVHV Parquet files (2026-01 to 2026-04), 1 Yellow Taxi Parquet file (2026-01) and `taxi_zone_lookup.csv` from `data/` (2.0 GB in total). Both trip schemas are mapped to a common schema.
2. **Cleanse:** drop nulls in the key columns, drop exact duplicates, derive `trip_time_sec`, keep rows with `trip_miles > 0`, `trip_time_sec > 0` and `fare >= 0`.
3. **Transform:** two inner joins to the zone lookup (pickup and drop-off), `speed_mph` via a Python UDF (null if outside (0, 100] mph, rounded to 2 decimals), plus `pickup_hour`, `pickup_dow` and `is_weekend`.
4. **Export:** write the 16-column result as Parquet.

## Environment

AMD Ryzen 7 5700U, 12 GiB RAM, Python 3.14.4, PySpark 4.2.0, Ray 2.58.0. Both clusters run on this single machine, each with 2 workers of 2 cores.

## How to run

Data goes in `data/`. Create a virtual environment and install `pyspark`, `ray[data]`, `pandas`, `pyarrow`, `psutil` and `matplotlib`.

**Spark cluster.** Start a standalone master at `spark://127.0.0.1:7077` with 2 workers, each with 2 cores and 4 GiB. The Master UI should show 2 alive workers.

```bash
mkdir -p logs
export SPARK_HOME=$(python -c "import pyspark,os;print(os.path.dirname(pyspark.__file__))")

nohup $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
  --host 127.0.0.1 --port 7077 --webui-port 8080 > logs/spark-master.log 2>&1 &

nohup $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --cores 2 --memory 4g --webui-port 8081 --work-dir /tmp/spark-worker1 \
  spark://127.0.0.1:7077 > logs/spark-worker1.log 2>&1 &

nohup $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --cores 2 --memory 4g --webui-port 8082 --work-dir /tmp/spark-worker2 \
  spark://127.0.0.1:7077 > logs/spark-worker2.log 2>&1 &

grep -i "registered" logs/spark-worker*.log   # both workers should register
```
The Master UI at http://127.0.0.1:8080 should then show 2 alive workers. To stop the cluster: pkill -f org.apache.spark.deploy

**Ray cluster** (leave this terminal running):

```bash
python ray_cluster.py            # writes .ray_address; dashboard at http://127.0.0.1:8265
```

**Pipelines, with resource monitoring:**

```bash
python monitor.py --label spark -- python spark_clean.py --out out/spark \
    --master spark://127.0.0.1:7077 --executor-cores 2 --executor-memory 3g

python monitor.py --label ray -- python ray_clean.py --out out/ray --address $(cat .ray_address)
```

**Parity check, UDF benchmark and comparison tables:**

```bash
python verify_parity.py --spark-out out/spark --ray-out out/ray

python udf_benchmark.py --engine spark --input out/spark --master spark://127.0.0.1:7077 \
    --executor-cores 2 --executor-memory 3g --skip-row-udf
python udf_benchmark.py --engine ray --input out/spark --address $(cat .ray_address)

python compare.py
```

`spark_clean.py` and `ray_clean.py` take `--sample` (a fraction of rows, for quick tests) and `--partitions`. `ray_clean.py` also takes `--passes`. All of these have defaults in `common.py` and the scripts themselves.

## Notes and limitations

- Results are from a single run per framework.
- CPU and memory are system-wide readings (psutil), not per process.
- Ray processed the data in 4 hash-bucket passes and Spark in one query, so the time comparison is between two tuned pipelines.
- Screenshots (Spark Master UI with 2 workers, Ray Dashboard cluster view) are in the report.
