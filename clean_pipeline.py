from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

try:
    import jdk4py

    os.environ.setdefault("JAVA_HOME", str(jdk4py.JAVA_HOME))
    os.environ["PATH"] = str(Path(jdk4py.JAVA_HOME) / "bin") + os.pathsep + os.environ["PATH"]
except ImportError:
    pass

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


BASE_DIR = Path(__file__).resolve().parent
READINGS_DEFAULT = BASE_DIR / "data" / "raw" / "parcel_readings.csv"
METADATA_DEFAULT = BASE_DIR / "data" / "raw" / "parcel_metadata.csv"


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("parcel-clean-pipeline")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def write_single_csv(df: DataFrame, output_path: Path) -> None:
    """Write Spark rows to one CSV file without a non-Spark dataframe conversion.

    Spark's native CSV writer needs winutils/HADOOP_HOME on local Windows. The
    transformation work still happens in Spark; this final collection step exists
    only to produce the exact single-file deliverable requested here.
    """
    output_path = output_path.resolve()
    if output_path.exists():
        output_path.unlink()

    columns = df.columns
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for row in df.toLocalIterator():
            writer.writerow([row[column] for column in columns])


def load_and_clean_readings(spark: SparkSession, path: Path) -> DataFrame:
    readings = (
        spark.read.option("header", True)
        .csv(str(path))
        .withColumnRenamed("date", "date_raw")
        .withColumnRenamed("sensor_status", "sensor_status_raw")
        .withColumn("parcel_id", F.trim("parcel_id"))
        .withColumn("ndvi_value", F.col("ndvi_value").cast("double"))
        .withColumn("temperature_c", F.col("temperature_c").cast("double"))
        .withColumn("rainfall_mm", F.col("rainfall_mm").cast("double"))
    )

    parsed_date = (
        F.when(
            F.col("date_raw").rlike(r"^\d{4}-\d{2}-\d{2}$"),
            F.to_date("date_raw", "yyyy-MM-dd"),
        )
        .when(
            F.col("date_raw").rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"),
            F.to_date("date_raw", "d/M/yyyy"),
        )
        .when(
            F.col("date_raw").rlike(r"^\d{1,2}-[A-Za-z]{3}-\d{4}$"),
            F.to_date("date_raw", "d-MMM-yyyy"),
        )
    )

    status_clean = F.lower(F.trim(F.col("sensor_status_raw")))

    return (
        readings.withColumn("date", parsed_date)
        .withColumn(
            "sensor_status",
            F.when(status_clean.isin("ok", "error"), status_clean).otherwise(
                F.lit("unknown")
            ),
        )
        .withColumn("is_bad_sensor", F.col("sensor_status") != F.lit("ok"))
        .withColumn("ndvi_value_raw", F.col("ndvi_value"))
        .withColumn(
            "ndvi_invalid",
            F.col("ndvi_value").isNull() | ~F.col("ndvi_value").between(0, 1),
        )
        .withColumn(
            "ndvi_value",
            F.when(F.col("ndvi_invalid"), F.lit(None).cast("double")).otherwise(
                F.col("ndvi_value")
            ),
        )
        .withColumn(
            "temperature_invalid",
            F.col("temperature_c").isNull() | ~F.col("temperature_c").between(-10, 50),
        )
        .withColumn(
            "temperature_c",
            F.when(
                F.col("temperature_invalid"), F.lit(None).cast("double")
            ).otherwise(F.col("temperature_c")),
        )
        .withColumn(
            "rainfall_invalid",
            F.col("rainfall_mm").isNull() | (F.col("rainfall_mm") < F.lit(0)),
        )
        .withColumn(
            "rainfall_mm",
            F.when(F.col("rainfall_invalid"), F.lit(None).cast("double")).otherwise(
                F.col("rainfall_mm")
            ),
        )
    )


def load_and_clean_metadata(spark: SparkSession, path: Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .csv(str(path))
        .withColumnRenamed("sowing_date", "sowing_date_raw")
        .withColumn("parcel_id", F.trim("parcel_id"))
        .withColumn("mill_id", F.upper(F.trim("mill_id")))
        .withColumn("crop_type", F.lower(F.trim("crop_type")))
        .withColumn("sowing_date", F.to_date("sowing_date_raw", "yyyy-MM-dd"))
        .withColumn("area_hectares", F.col("area_hectares").cast("double"))
        .withColumn(
            "area_invalid",
            F.col("area_hectares").isNull() | (F.col("area_hectares") <= F.lit(0)),
        )
        .withColumn(
            "area_hectares",
            F.when(F.col("area_invalid"), F.lit(None).cast("double")).otherwise(
                F.col("area_hectares")
            ),
        )
        .drop("sowing_date_raw")
    )


def aggregate_readings(readings: DataFrame) -> DataFrame:
    return (
        readings.groupBy("parcel_id", "date")
        .agg(
            F.avg("ndvi_value").alias("ndvi_value"),
            F.avg("temperature_c").alias("temperature_c"),
            F.avg("rainfall_mm").alias("rainfall_mm"),
            F.max(F.when(F.col("sensor_status") == "error", 2).when(F.col("sensor_status") == "unknown", 1).otherwise(0)).alias("status_rank"),
            F.count(F.lit(1)).alias("source_rows"),
            F.max(F.col("is_bad_sensor").cast("int")).cast("boolean").alias("had_bad_sensor"),
            F.max(F.col("ndvi_invalid").cast("int")).cast("boolean").alias("had_invalid_ndvi"),
            F.max(F.col("temperature_invalid").cast("int")).cast("boolean").alias("had_invalid_temperature"),
            F.max(F.col("rainfall_invalid").cast("int")).cast("boolean").alias("had_invalid_rainfall"),
        )
        .withColumn(
            "sensor_status",
            F.when(F.col("status_rank") == 2, F.lit("error"))
            .when(F.col("status_rank") == 1, F.lit("unknown"))
            .otherwise(F.lit("ok")),
        )
        .withColumn("is_bad_sensor", F.col("sensor_status") != F.lit("ok"))
        .withColumn("was_duplicate_key", F.col("source_rows") > F.lit(1))
        .drop("status_rank")
    )


def build_timeseries(spark: SparkSession, readings: DataFrame, metadata: DataFrame) -> DataFrame:
    aggregated = aggregate_readings(readings)
    date_bounds = aggregated.agg(
        F.min("date").alias("min_date"), F.max("date").alias("max_date")
    )
    dates = date_bounds.select(
        F.explode(F.sequence("min_date", "max_date", F.expr("interval 1 day"))).alias(
            "date"
        )
    )
    panel = metadata.select("parcel_id").crossJoin(dates)

    timeseries = (
        panel.join(aggregated, ["parcel_id", "date"], "left")
        .join(metadata, "parcel_id", "left")
        .withColumn("source_rows", F.coalesce("source_rows", F.lit(0)).cast("int"))
        .withColumn("missing_reading", F.col("source_rows") == F.lit(0))
        .withColumn("sensor_status", F.coalesce("sensor_status", F.lit("missing")))
    )

    boolean_columns = [
        "is_bad_sensor",
        "was_duplicate_key",
        "had_bad_sensor",
        "had_invalid_ndvi",
        "had_invalid_temperature",
        "had_invalid_rainfall",
    ]
    for column in boolean_columns:
        timeseries = timeseries.withColumn(
            column, F.coalesce(F.col(column), F.lit(False))
        )

    return timeseries.select(
        "parcel_id",
        "date",
        "mill_id",
        "crop_type",
        "sowing_date",
        "area_hectares",
        "ndvi_value",
        "temperature_c",
        "rainfall_mm",
        "sensor_status",
        "is_bad_sensor",
        "missing_reading",
        "source_rows",
        "was_duplicate_key",
        "had_bad_sensor",
        "had_invalid_ndvi",
        "had_invalid_temperature",
        "had_invalid_rainfall",
        "area_invalid",
    ).orderBy("parcel_id", "date")


def analyze_by_crop(timeseries: DataFrame) -> DataFrame:
    observed_ok = (
        timeseries.filter((F.col("sensor_status") == "ok") & F.col("ndvi_value").isNotNull())
        .withColumn("days_from_sowing", F.datediff("date", "sowing_date"))
    )
    per_crop = observed_ok.groupBy("crop_type").agg(
        F.avg(
            F.when(F.col("days_from_sowing").between(-30, -1), F.col("ndvi_value"))
        ).alias("mean_ndvi_before"),
        F.avg(
            F.when(F.col("days_from_sowing").between(0, 30), F.col("ndvi_value"))
        ).alias("mean_ndvi_after"),
    )
    parcel_counts = (
        observed_ok.filter(F.col("days_from_sowing").between(-30, 30))
        .select("crop_type", "parcel_id")
        .distinct()
        .groupBy("crop_type")
        .agg(F.count("parcel_id").alias("n_parcels"))
    )
    return (
        per_crop.join(parcel_counts, "crop_type", "left")
        .withColumn("n_parcels", F.coalesce("n_parcels", F.lit(0)).cast("int"))
        .orderBy("crop_type")
    )


def audit_metrics(
    readings_raw: DataFrame, readings: DataFrame, metadata: DataFrame, timeseries: DataFrame
) -> DataFrame:
    return readings_raw.select(
        F.count(F.lit(1)).alias("reading_rows"),
        F.sum(F.when(F.col("date_raw").rlike(r"^\d{4}-\d{2}-\d{2}$"), 1).otherwise(0)).alias("iso_date_rows"),
        F.sum(F.when(F.col("date_raw").rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"), 1).otherwise(0)).alias("slash_date_rows"),
        F.sum(F.when(F.col("date_raw").rlike(r"^\d{1,2}-[A-Za-z]{3}-\d{4}$"), 1).otherwise(0)).alias("month_name_date_rows"),
    ).crossJoin(
        readings.select(
            F.sum(F.col("ndvi_invalid").cast("int")).alias("invalid_ndvi_rows"),
            F.sum(F.col("temperature_invalid").cast("int")).alias("invalid_temperature_rows"),
            F.sum(F.col("rainfall_invalid").cast("int")).alias("invalid_rainfall_rows"),
            F.sum(F.when(F.col("sensor_status") == "unknown", 1).otherwise(0)).alias("unknown_sensor_rows"),
        )
    ).crossJoin(
        metadata.select(F.count(F.lit(1)).alias("metadata_rows"))
    ).crossJoin(
        timeseries.select(
            F.count(F.lit(1)).alias("output_rows"),
            F.sum(F.col("missing_reading").cast("int")).alias("missing_reading_rows"),
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--readings", type=Path, default=READINGS_DEFAULT)
    parser.add_argument("--metadata", type=Path, default=METADATA_DEFAULT)
    parser.add_argument(
        "--output", type=Path, default=Path("cleaned_parcel_timeseries.csv")
    )
    parser.add_argument("--analysis-output", type=Path, default=Path("crop_ndvi_summary.csv"))
    args = parser.parse_args()

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        readings_raw = (
            spark.read.option("header", True)
            .csv(str(args.readings))
            .withColumnRenamed("date", "date_raw")
        )
        readings = load_and_clean_readings(spark, args.readings)
        metadata = load_and_clean_metadata(spark, args.metadata)
        timeseries = build_timeseries(spark, readings, metadata)
        analysis = analyze_by_crop(timeseries)

        write_single_csv(timeseries, args.output)
        write_single_csv(analysis, args.analysis_output)

        print(f"Wrote {timeseries.count():,} rows to {args.output.resolve()}")
        print(f"Wrote crop summary to {args.analysis_output.resolve()}")
        analysis.show(truncate=False)
        audit_metrics(readings_raw, readings, metadata, timeseries).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
