import os
import shutil
import sys
import time
import pandas as pd
import ray
import ray.data
from ray.data.aggregate import Count, Mean
import common

BATCH = 100_000


def normalize(df: pd.DataFrame, cols: dict, source: str) -> pd.DataFrame:
    df = df[list(cols)].rename(columns=cols)
    for c in common.DT_COLS:
        df[c] = pd.to_datetime(df[c]).astype("datetime64[us]")
    for c in common.ID_COLS + common.NUM_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    df["source"] = source
    return df


def cleanse(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=common.KEY_COLS).copy()
    for c in common.ID_COLS:
        df[c] = df[c].astype("int64")
    df["row_hash"] = pd.util.hash_pandas_object(
        df[common.KEY_COLS + ["source"]], index=False
    ).astype("int64")
    return df


def drop_dupes(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset="row_hash")


def transform(df: pd.DataFrame, zones: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["trip_time_sec"] = (
        (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds().astype("int64")
    )
    df = df[(df["trip_miles"] > 0) & (df["trip_time_sec"] > 0) & (df["fare"] >= 0)]

    pu = zones.rename(columns={"LocationID": "PULocationID", "Borough": "pu_borough", "Zone": "pu_zone"})
    do = zones.rename(columns={"LocationID": "DOLocationID", "Borough": "do_borough", "Zone": "do_zone"})
    df = df.merge(pu[["PULocationID", "pu_borough", "pu_zone"]], on="PULocationID", how="inner")
    df = df.merge(do[["DOLocationID", "do_borough", "do_zone"]], on="DOLocationID", how="inner")

    df["speed_mph"] = common.compute_speed(df["trip_miles"], df["trip_time_sec"])
    df = df.dropna(subset=["speed_mph"])
    df["pickup_hour"] = df["pickup_datetime"].dt.hour.astype("int32")
    dow = df["pickup_datetime"].dt.dayofweek.to_numpy(dtype="int64")
    df["pickup_dow"] = ((dow + 1) % 7 + 1).astype("int32")
    df["is_weekend"] = ((df["pickup_dow"].to_numpy() == 1) | (df["pickup_dow"].to_numpy() == 7)).astype("int32")
    return df[common.FINAL_COLS]


def ingest(data_dir):
    parts = []
    for prefix, cols in common.SOURCES.items():
        files = common.list_files(data_dir, prefix)
        if not files:
            continue
        ds = ray.data.read_parquet(files, columns=list(cols))
        ds = ds.map_batches(normalize, batch_format="pandas", batch_size=BATCH,
                            fn_kwargs={"cols": cols, "source": prefix})
        parts.append(ds)
    if not parts:
        sys.exit(f"No parquet files found in {data_dir}")
    return parts[0].union(*parts[1:]) if len(parts) > 1 else parts[0]


def main():
    p = common.base_parser("Ray pipeline")
    p.add_argument("--address", default=None, help="Ray cluster address (default: start local Ray)")
    p.add_argument("--cpus", type=int, default=None, help="CPUs for local Ray")
    args = p.parse_args()

    if args.address:
        ray.init(address=args.address, runtime_env={
            "working_dir": ".",
            "excludes": ["data", "out", "results", "logs", "venv", ".venv", "*.parquet", "*.csv"],
        })
    else:
        ray.init(num_cpus=args.cpus, include_dashboard=False)

    ray.data.DataContext.get_current().enable_progress_bars = True

    rows_in = common.raw_row_count(args.data_dir)
    zones = pd.read_csv(args.zones, keep_default_na=False)
    if os.path.exists(args.out):
        shutil.rmtree(args.out)

    t0 = time.perf_counter()

    ds = ingest(args.data_dir)
    if args.sample < 1.0:
        ds = ds.random_sample(args.sample, seed=42)
    ds = ds.map_batches(cleanse, batch_format="pandas", batch_size=BATCH)
    try:
        ds = ds.repartition(args.partitions, keys=["row_hash"], shuffle=True)
        ds = ds.map_batches(drop_dupes, batch_format="pandas", batch_size=None)
    except (TypeError, ValueError):
        print("WARNING: this Ray version lacks repartition(keys=...). "
              "Only per-block dedup is done. Upgrade: pip install -U 'ray[data]'")
        ds = ds.map_batches(drop_dupes, batch_format="pandas", batch_size=BATCH)

    ds = ds.map_batches(transform, batch_format="pandas", batch_size=BATCH,
                        fn_kwargs={"zones": zones})

    ds.write_parquet(args.out)
    elapsed = time.perf_counter() - t0

    out_ds = ray.data.read_parquet(args.out)
    rows_out = out_ds.count()
    hourly = (
        ray.data.read_parquet(args.out, columns=["pickup_hour", "speed_mph"])
        .groupby("pickup_hour").aggregate(Mean("speed_mph"), Count())
        .to_pandas().sort_values("pickup_hour")
    )
    print(hourly.to_string(index=False))

    print(f"\n[RAY] rows_in={rows_in:,} rows_out={rows_out:,} elapsed={elapsed:.1f}s")
    common.save_result("ray", {
        "engine": "ray", "sample": args.sample, "rows_in_raw": rows_in,
        "rows_out": rows_out, "elapsed_sec": round(elapsed, 2),
    })
    ray.shutdown()


if __name__ == "__main__":
    main()
