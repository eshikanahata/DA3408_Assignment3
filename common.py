import argparse
import glob
import json
import os
import pandas as pd
import pyarrow.parquet as pq
SOURCES = {
    "fhvhv": { 
        "pickup_datetime": "pickup_datetime",
        "dropoff_datetime": "dropoff_datetime",
        "PULocationID": "PULocationID",
        "DOLocationID": "DOLocationID",
        "trip_miles": "trip_miles",
        "base_passenger_fare": "fare",
    },
    "yellow": { 
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "PULocationID",
        "DOLocationID": "DOLocationID",
        "trip_distance": "trip_miles",
        "fare_amount": "fare",
    },
}

DT_COLS = ["pickup_datetime", "dropoff_datetime"]
ID_COLS = ["PULocationID", "DOLocationID"]
NUM_COLS = ["trip_miles", "fare"]
KEY_COLS = DT_COLS + ID_COLS + NUM_COLS  

MAX_SPEED_MPH = 100.0  

FINAL_COLS = [
    "pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID",
    "pu_borough", "pu_zone", "do_borough", "do_zone",
    "trip_miles", "trip_time_sec", "fare", "speed_mph",
    "pickup_hour", "pickup_dow", "is_weekend", "source",
]


def compute_speed(miles: pd.Series, secs: pd.Series) -> pd.Series:
    hours = secs.astype("float64") / 3600.0
    speed = miles.astype("float64") / hours
    speed = speed.where((speed > 0) & (speed <= MAX_SPEED_MPH))
    return speed.round(2)


def list_files(data_dir, prefix):
    return sorted(glob.glob(os.path.join(data_dir, f"{prefix}_tripdata_*.parquet")))


def raw_row_count(data_dir):
    total = 0
    for prefix in SOURCES:
        for f in list_files(data_dir, prefix):
            total += pq.ParquetFile(f).metadata.num_rows
    return total


def base_parser(description):
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--data-dir", default="data", help="folder with the parquet files")
    p.add_argument("--zones", default="data/taxi_zone_lookup.csv", help="taxi zone lookup CSV")
    p.add_argument("--out", required=True, help="output parquet directory")
    p.add_argument("--sample", type=float, default=1.0,
                   help="fraction of rows to use (e.g. 0.01 for a quick smoke test)")
    p.add_argument("--partitions", type=int, default=64, help="shuffle / repartition count")
    return p


def save_result(engine, result):
    os.makedirs("results", exist_ok=True)
    with open(os.path.join("results", f"{engine}.json"), "w") as f:
        json.dump(result, f, indent=2)


def add_spark_args(p):
    p.add_argument("--master", default="local[*]", help="e.g. spark://127.0.0.1:7077")
    p.add_argument("--memory", default="4g", help="driver memory")
    p.add_argument("--executor-memory", default="3g", help="memory per executor")
    p.add_argument("--executor-cores", type=int, default=2, help="cores per executor")


def make_spark(app, args):
    """One place to build the SparkSession so pipeline + UDF benchmark match."""
    import sys
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.appName(app)
        .master(args.master)
        .config("spark.driver.memory", args.memory)
        .config("spark.executor.memory", args.executor_memory)
        .config("spark.executor.cores", str(args.executor_cores))
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .config("spark.sql.shuffle.partitions", str(getattr(args, "partitions", 64)))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "100000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addPyFile(os.path.abspath("common.py"))  
    return spark
