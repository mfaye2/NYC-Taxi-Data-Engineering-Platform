"""Generate a data-quality profile for NYC Yellow Taxi data.

Example:
    python src/quality/profile_trips.py \
        --file "data/raw/trips/year=2025/month=01/yellow_tripdata_2025-01.parquet" \
        --year 2025 \
        --month 1
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


REQUIRED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
]


def convert_to_python_value(value: Any) -> Any:
    """Convert pandas and NumPy values into JSON-compatible values."""
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if hasattr(value, "item"):
        return value.item()

    return value


def generate_profile(
    file_path: Path,
    expected_year: int,
    expected_month: int,
) -> dict[str, Any]:
    """Read the Parquet file and calculate quality indicators."""
    if not file_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {file_path}")

    print(f"Lecture du fichier : {file_path}")
    dataframe = pd.read_parquet(file_path)

    missing_required_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in dataframe.columns
    ]

    pickup_datetime = pd.to_datetime(
        dataframe["tpep_pickup_datetime"],
        errors="coerce",
    )
    dropoff_datetime = pd.to_datetime(
        dataframe["tpep_dropoff_datetime"],
        errors="coerce",
    )

    trip_duration_minutes = (
        dropoff_datetime - pickup_datetime
    ).dt.total_seconds() / 60

    rows_outside_expected_month = ~(
        (pickup_datetime.dt.year == expected_year)
        & (pickup_datetime.dt.month == expected_month)
    )

    null_counts = dataframe.isna().sum()
    null_percentages = (
        dataframe.isna().mean() * 100
    ).round(2)

    profile = {
        "file": str(file_path),
        "file_size_mb": round(
            file_path.stat().st_size / (1024 ** 2),
            2,
        ),
        "expected_period": {
            "year": expected_year,
            "month": expected_month,
        },
        "dataset": {
            "row_count": int(len(dataframe)),
            "column_count": int(len(dataframe.columns)),
            "duplicate_rows": int(dataframe.duplicated().sum()),
        },
        "schema": {
            "columns": list(dataframe.columns),
            "missing_required_columns": missing_required_columns,
        },
        "null_values": {
            column: {
                "count": int(null_counts[column]),
                "percentage": float(null_percentages[column]),
            }
            for column in dataframe.columns
            if null_counts[column] > 0
        },
        "date_quality": {
            "null_pickup_datetime": int(pickup_datetime.isna().sum()),
            "null_dropoff_datetime": int(dropoff_datetime.isna().sum()),
            "dropoff_before_pickup": int(
                (trip_duration_minutes < 0).sum()
            ),
            "zero_duration_trips": int(
                (trip_duration_minutes == 0).sum()
            ),
            "trips_longer_than_24_hours": int(
                (trip_duration_minutes > 24 * 60).sum()
            ),
            "rows_outside_expected_month": int(
                rows_outside_expected_month.sum()
            ),
            "minimum_pickup_datetime": convert_to_python_value(
                pickup_datetime.min()
            ),
            "maximum_pickup_datetime": convert_to_python_value(
                pickup_datetime.max()
            ),
        },
        "distance_quality": {
            "negative_trip_distance": int(
                (dataframe["trip_distance"] < 0).sum()
            ),
            "zero_trip_distance": int(
                (dataframe["trip_distance"] == 0).sum()
            ),
            "trip_distance_over_100_miles": int(
                (dataframe["trip_distance"] > 100).sum()
            ),
            "maximum_trip_distance": convert_to_python_value(
                dataframe["trip_distance"].max()
            ),
        },
        "amount_quality": {
            "negative_fare_amount": int(
                (dataframe["fare_amount"] < 0).sum()
            ),
            "negative_tip_amount": int(
                (dataframe["tip_amount"] < 0).sum()
            ),
            "negative_total_amount": int(
                (dataframe["total_amount"] < 0).sum()
            ),
            "total_amount_over_1000": int(
                (dataframe["total_amount"] > 1000).sum()
            ),
            "maximum_total_amount": convert_to_python_value(
                dataframe["total_amount"].max()
            ),
        },
        "location_quality": {
            "invalid_pickup_location_ids": int(
                (
                    (dataframe["PULocationID"] < 1)
                    | (dataframe["PULocationID"] > 265)
                ).sum()
            ),
            "invalid_dropoff_location_ids": int(
                (
                    (dataframe["DOLocationID"] < 1)
                    | (dataframe["DOLocationID"] > 265)
                ).sum()
            ),
        },
    }

    return profile


def save_profile(
    profile: dict[str, Any],
    output_path: Path,
) -> None:
    """Save the profile as a formatted JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as output_file:
        json.dump(
            profile,
            output_file,
            indent=4,
            ensure_ascii=False,
        )

    print(f"Rapport enregistré : {output_path}")


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Créer un profil qualité d'un fichier NYC Taxi."
    )

    parser.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Chemin du fichier Parquet.",
    )

    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Année attendue dans le fichier.",
    )

    parser.add_argument(
        "--month",
        type=int,
        required=True,
        choices=range(1, 13),
        help="Mois attendu, entre 1 et 12.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/data_profile_2025_01.json"),
        help="Chemin du rapport JSON.",
    )

    return parser.parse_args()


def main() -> None:
    """Run the profiling command."""
    arguments = parse_arguments()

    profile = generate_profile(
        file_path=arguments.file,
        expected_year=arguments.year,
        expected_month=arguments.month,
    )

    save_profile(
        profile=profile,
        output_path=arguments.output,
    )

    print("\n=== RÉSUMÉ ===")
    print(f"Lignes : {profile['dataset']['row_count']:,}")
    print(f"Doublons : {profile['dataset']['duplicate_rows']:,}")
    print(
        "Dates hors période : "
        f"{profile['date_quality']['rows_outside_expected_month']:,}"
    )
    print(
        "Distances supérieures à 100 miles : "
        f"{profile['distance_quality']['trip_distance_over_100_miles']:,}"
    )
    print(
        "Montants totaux négatifs : "
        f"{profile['amount_quality']['negative_total_amount']:,}"
    )


if __name__ == "__main__":
    main()