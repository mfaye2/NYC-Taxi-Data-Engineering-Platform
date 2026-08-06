"""Transform and enrich NYC Yellow Taxi trip data.

This local transformation is used to validate the business logic before
implementing the equivalent AWS Glue PySpark job.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd


LOGGER = logging.getLogger(__name__)

COLUMN_MAPPING = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "Airport_fee": "airport_fee",
}


def load_trip_data(file_path: Path) -> pd.DataFrame:
    """Load a Parquet trip-data file."""

    if not file_path.exists():
        raise FileNotFoundError(f"Fichier de trajets introuvable : {file_path}")

    LOGGER.info("Lecture des trajets : %s", file_path)
    return pd.read_parquet(file_path)


def load_zone_lookup(file_path: Path) -> pd.DataFrame:
    """Load and standardize the taxi-zone lookup table."""

    if not file_path.exists():
        raise FileNotFoundError(f"Fichier des zones introuvable : {file_path}")

    LOGGER.info("Lecture des zones : %s", file_path)

    zones = pd.read_csv(file_path)

    expected_columns = {
        "LocationID",
        "Borough",
        "Zone",
        "service_zone",
    }

    missing_columns = expected_columns.difference(zones.columns)

    if missing_columns:
        raise ValueError(
            f"Colonnes manquantes dans le fichier des zones : "
            f"{sorted(missing_columns)}"
        )

    zones = zones.rename(
        columns={
            "LocationID": "location_id",
            "Borough": "borough",
            "Zone": "zone",
        }
    )

    return zones


def standardize_trip_columns(trips: pd.DataFrame) -> pd.DataFrame:
    """Rename important trip columns using snake_case names."""

    return trips.rename(columns=COLUMN_MAPPING).copy()


def add_trip_metrics(trips: pd.DataFrame) -> pd.DataFrame:
    """Create analytical columns from pickup and dropoff information."""

    trips["pickup_datetime"] = pd.to_datetime(
        trips["pickup_datetime"],
        errors="coerce",
    )

    trips["dropoff_datetime"] = pd.to_datetime(
        trips["dropoff_datetime"],
        errors="coerce",
    )

    trips["trip_duration_minutes"] = (
        trips["dropoff_datetime"] - trips["pickup_datetime"]
    ).dt.total_seconds() / 60

    trips["pickup_date"] = trips["pickup_datetime"].dt.date
    trips["pickup_year"] = trips["pickup_datetime"].dt.year
    trips["pickup_month"] = trips["pickup_datetime"].dt.month
    trips["pickup_day"] = trips["pickup_datetime"].dt.day
    trips["pickup_hour"] = trips["pickup_datetime"].dt.hour
    trips["pickup_weekday"] = trips["pickup_datetime"].dt.day_name()

    trips["is_weekend"] = (
        trips["pickup_datetime"].dt.dayofweek >= 5
    )

    trips["is_peak_hour"] = (
        trips["pickup_hour"].between(7, 9)
        | trips["pickup_hour"].between(16, 19)
    )

    valid_duration = trips["trip_duration_minutes"] > 0

    trips["average_speed_mph"] = pd.NA

    trips.loc[valid_duration, "average_speed_mph"] = (
        trips.loc[valid_duration, "trip_distance"]
        / (
            trips.loc[valid_duration, "trip_duration_minutes"]
            / 60
        )
    )

    trips["is_refund_or_reversal"] = (
        (trips["fare_amount"] < 0)
        | (trips["total_amount"] < 0)
    )

    trips["is_anomalous_trip"] = (
        (trips["trip_duration_minutes"] <= 0)
        | (trips["trip_duration_minutes"] > 24 * 60)
        | (trips["trip_distance"] > 100)
        | (trips["total_amount"] > 1000)
    )

    trips["processing_timestamp"] = pd.Timestamp.now(tz="UTC")

    return trips


def enrich_with_zones(
    trips: pd.DataFrame,
    zones: pd.DataFrame,
) -> pd.DataFrame:
    """Add pickup and dropoff zone information."""

    pickup_zones = zones.rename(
        columns={
            "location_id": "pickup_location_id",
            "borough": "pickup_borough",
            "zone": "pickup_zone",
            "service_zone": "pickup_service_zone",
        }
    )

    dropoff_zones = zones.rename(
        columns={
            "location_id": "dropoff_location_id",
            "borough": "dropoff_borough",
            "zone": "dropoff_zone",
            "service_zone": "dropoff_service_zone",
        }
    )

    enriched = trips.merge(
        pickup_zones,
        on="pickup_location_id",
        how="left",
        validate="many_to_one",
    )

    enriched = enriched.merge(
        dropoff_zones,
        on="dropoff_location_id",
        how="left",
        validate="many_to_one",
    )

    return enriched


def save_sample(
    dataframe: pd.DataFrame,
    output_path: Path,
    sample_size: int,
) -> None:
    """Save a small transformed sample as Parquet."""

    output_path.parent.mkdir(parents=True, exist_ok=True)

    sample = dataframe.head(sample_size).copy()
    sample.to_parquet(output_path, index=False)

    LOGGER.info(
        "Échantillon enregistré : %s (%s lignes)",
        output_path,
        len(sample),
    )


def transform_trip_data(
    trips_path: Path,
    zones_path: Path,
) -> pd.DataFrame:
    """Run the complete local transformation."""

    trips = load_trip_data(trips_path)
    zones = load_zone_lookup(zones_path)

    trips = standardize_trip_columns(trips)
    trips = add_trip_metrics(trips)
    trips = enrich_with_zones(trips, zones)

    return trips


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(
        description="Transformer et enrichir les données NYC Taxi."
    )

    parser.add_argument(
        "--trips",
        type=Path,
        required=True,
        help="Chemin du fichier mensuel Parquet.",
    )

    parser.add_argument(
        "--zones",
        type=Path,
        default=Path("data/raw/zones/taxi_zone_lookup.csv"),
        help="Chemin du référentiel des zones.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/sample/transformed_trips_sample.parquet"),
        help="Chemin de l'échantillon de sortie.",
    )

    parser.add_argument(
        "--sample-size",
        type=int,
        default=10_000,
        help="Nombre de lignes à enregistrer dans l'échantillon.",
    )

    return parser.parse_args()


def main() -> None:
    """Run the transformation."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    arguments = parse_arguments()

    transformed = transform_trip_data(
        trips_path=arguments.trips,
        zones_path=arguments.zones,
    )

    print("\n=== TRANSFORMATION TERMINÉE ===")
    print(f"Lignes transformées : {len(transformed):,}")
    print(f"Colonnes finales : {len(transformed.columns)}")

    print("\n=== ZONES DE DÉPART MANQUANTES ===")
    print(f"{transformed['pickup_zone'].isna().sum():,}")

    print("\n=== ZONES D'ARRIVÉE MANQUANTES ===")
    print(f"{transformed['dropoff_zone'].isna().sum():,}")

    print("\n=== TRAJETS MARQUÉS COMME ANORMAUX ===")
    print(f"{transformed['is_anomalous_trip'].sum():,}")

    print("\n=== EXEMPLE DE COLONNES ENRICHIES ===")
    print(
        transformed[
            [
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_borough",
                "pickup_zone",
                "dropoff_borough",
                "dropoff_zone",
                "trip_distance",
                "trip_duration_minutes",
                "average_speed_mph",
                "total_amount",
                "is_anomalous_trip",
            ]
        ].head()
    )

    save_sample(
        dataframe=transformed,
        output_path=arguments.output,
        sample_size=arguments.sample_size,
    )


if __name__ == "__main__":
    main()