import os
import shutil
import sys
import time
from functools import reduce

os.environ.setdefault("PYSPARK_PYTHON", sys.executable) 

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

import common


@pandas_udf(DoubleType())
def speed_udf(miles: pd.Series, secs: pd.Series) -> pd.Series:
    return common.compute_speed(miles, secs)


def ingest(spark, data_dir):
    frames = []
    for prefix, cols in common.SOURCES.items():
        files = common.list_files(data_dir, prefix)
        if not files:
            continue
        df = spark.read.parquet(*files)
        df = df.select(*[F.col(src).alias(dst) for src, dst in cols.items()])
        frames.append(df.withColumn("source", F.lit(prefix)))
    if not frames:
        sys.exit(f"No parquet files found in {data_dir}")
    frames = [
        f.withColumn("pickup_datetime", F.col("pickup_datetime").cast("timestamp"))
         .withColumn("dropoff_datetime", F.col("dropoff_datetime").cast("timestamp"))
         .withColumn("PULocationID", F.col("PULocationID").cast("int"))
         .withColumn("DOLocationID", F.col("DOLocationID").cast("int"))
         .withColumn("trip_miles", F.col("trip_miles").cast("double"))
         .withColumn("fare", F.col("fare").cast("double"))
        for f in frames
    ]
    return reduce(lambda a, b: a.unionByName(b), frames)


def main():
    p = common.base_parser("Spark pipeline")
    common.add_spark_args(p)
    args = p.parse_args()

    spark = common.make_spark("A3-Spark", args)

    rows_in = common.raw_row_count(args.data_dir)
    if os.path.exists(args.out):
        shutil.rmtree(args.out)

    t0 = time.perf_counter()
    df = ingest(spark, args.data_dir)
    if args.sample < 1.0:
        df = df.sample(fraction=args.sample, seed=42)

    df = df.dropna(subset=common.KEY_COLS).dropDuplicates()
    df = df.withColumn(
        "trip_time_sec",
        F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"),
    ).filter(
        (F.col("trip_miles") > 0) & (F.col("trip_time_sec") > 0) & (F.col("fare") >= 0)
    )

    zones = spark.read.csv(args.zones, header=True, inferSchema=True)
    pu = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pu_borough"),
        F.col("Zone").alias("pu_zone"),
    )
    do = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("do_borough"),
        F.col("Zone").alias("do_zone"),
    )
    df = df.join(pu, "PULocationID", "inner").join(do, "DOLocationID", "inner")

    df = (
        df.withColumn("speed_mph", speed_udf("trip_miles", "trip_time_sec"))
        .filter(F.col("speed_mph").isNotNull())
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_dow", F.dayofweek("pickup_datetime"))  # 1=Sun..7=Sat
        .withColumn("is_weekend", F.col("pickup_dow").isin(1, 7).cast("int"))
        .select(*common.FINAL_COLS)
    )

    df.write.mode("overwrite").parquet(args.out)
    elapsed = time.perf_counter() - t0

    # Post-run summary (NOT timed)
    out_df = spark.read.parquet(args.out)
    rows_out = out_df.count()
    hourly = (
        out_df.groupBy("pickup_hour")
        .agg(F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"), F.count("*").alias("trips"))
        .orderBy("pickup_hour")
    )
    hourly.show(24)

    print(f"\n[SPARK] rows_in={rows_in:,} rows_out={rows_out:,} elapsed={elapsed:.1f}s")
    common.save_result("spark", {
        "engine": "spark", "sample": args.sample, "rows_in_raw": rows_in,
        "rows_out": rows_out, "elapsed_sec": round(elapsed, 2),
    })
    spark.stop()


if __name__ == "__main__":
    main()
