"""AWS Glue PySpark job for NYC Yellow Taxi transformations."""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


ARGS = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_TRIPS_PATH",
        "SOURCE_ZONES_PATH",
        "CURATED_OUTPUT_PATH",
        "REJECTED_OUTPUT_PATH",
    ],
)


spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(ARGS["JOB_NAME"], ARGS)


def standardize_trip_columns(df: DataFrame) -> DataFrame:
    """Rename source columns using consistent snake_case names."""

    return (
        df.withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed(
            "tpep_pickup_datetime",
            "pickup_datetime",
        )
        .withColumnRenamed(
            "tpep_dropoff_datetime",
            "dropoff_datetime",
        )
        .withColumnRenamed("RatecodeID", "rate_code_id")
        .withColumnRenamed(
            "PULocationID",
            "pickup_location_id",
        )
        .withColumnRenamed(
            "DOLocationID",
            "dropoff_location_id",
        )
        .withColumnRenamed("Airport_fee", "airport_fee")
    )


def add_trip_metrics(df: DataFrame) -> DataFrame:
    """Create analytical and quality-control columns."""

    df = df.withColumn(
        "trip_duration_minutes",
        (
            F.unix_timestamp("dropoff_datetime")
            - F.unix_timestamp("pickup_datetime")
        )
        / 60.0,
    )

    df = (
        df.withColumn(
            "pickup_date",
            F.to_date("pickup_datetime"),
        )
        .withColumn(
            "pickup_year",
            F.year("pickup_datetime"),
        )
        .withColumn(
            "pickup_month",
            F.month("pickup_datetime"),
        )
        .withColumn(
            "pickup_day",
            F.dayofmonth("pickup_datetime"),
        )
        .withColumn(
            "pickup_hour",
            F.hour("pickup_datetime"),
        )
        .withColumn(
            "pickup_weekday",
            F.date_format(
                "pickup_datetime",
                "EEEE",
            ),
        )
    )

    df = df.withColumn(
        "is_weekend",
        F.dayofweek("pickup_datetime").isin(1, 7),
    )

    df = df.withColumn(
        "is_peak_hour",
        (
            F.col("pickup_hour").between(7, 9)
            | F.col("pickup_hour").between(16, 19)
        ),
    )

    df = df.withColumn(
        "average_speed_mph",
        F.when(
            F.col("trip_duration_minutes") > 0,
            F.col("trip_distance")
            / (F.col("trip_duration_minutes") / 60.0),
        ),
    )

    df = df.withColumn(
        "is_refund_or_reversal",
        (
            (F.col("fare_amount") < 0)
            | (F.col("total_amount") < 0)
        ),
    )

    df = df.withColumn(
        "is_anomalous_trip",
        (
            (F.col("trip_duration_minutes") <= 0)
            | (F.col("trip_duration_minutes") > 1440)
            | (F.col("trip_distance") > 100)
            | (F.col("total_amount") > 1000)
        ),
    )

    df = df.withColumn(
        "processing_timestamp",
        F.current_timestamp(),
    )

    return df


def prepare_pickup_zones(zones: DataFrame) -> DataFrame:
    """Prepare zone lookup for pickup join."""

    return zones.select(
        F.col("LocationID").alias("pickup_location_id"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
        F.col("service_zone").alias(
            "pickup_service_zone"
        ),
    )


def prepare_dropoff_zones(zones: DataFrame) -> DataFrame:
    """Prepare zone lookup for dropoff join."""

    return zones.select(
        F.col("LocationID").alias("dropoff_location_id"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("service_zone").alias(
            "dropoff_service_zone"
        ),
    )


trips = spark.read.parquet(
    ARGS["SOURCE_TRIPS_PATH"]
)

zones = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(ARGS["SOURCE_ZONES_PATH"])
)

trips = standardize_trip_columns(trips)
trips = add_trip_metrics(trips)

pickup_zones = prepare_pickup_zones(zones)
dropoff_zones = prepare_dropoff_zones(zones)

enriched = (
    trips.join(
        pickup_zones,
        on="pickup_location_id",
        how="left",
    )
    .join(
        dropoff_zones,
        on="dropoff_location_id",
        how="left",
    )
)

curated = enriched.filter(
    ~F.col("is_anomalous_trip")
)

rejected = enriched.filter(
    F.col("is_anomalous_trip")
)

(
    curated.write.mode("overwrite")
    .partitionBy(
        "pickup_year",
        "pickup_month",
    )
    .parquet(
        ARGS["CURATED_OUTPUT_PATH"]
    )
)

(
    rejected.write.mode("overwrite")
    .parquet(
        ARGS["REJECTED_OUTPUT_PATH"]
    )
)

print(
    f"Curated rows: {curated.count()}"
)

print(
    f"Rejected rows: {rejected.count()}"
)

job.commit()