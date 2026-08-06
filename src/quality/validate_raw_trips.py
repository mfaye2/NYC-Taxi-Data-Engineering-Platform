"""Validate raw NYC Yellow Taxi data with Great Expectations."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import great_expectations as gx
import pandas as pd


EXPECTED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
]


def validate_raw_file(file_path: Path) -> bool:
    """Validate one raw Parquet file."""

    if not file_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {file_path}")

    dataframe = pd.read_parquet(file_path)

    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas(
        name="nyc_taxi_pandas_source"
    )

    data_asset = data_source.add_dataframe_asset(
        name="yellow_taxi_raw_asset"
    )

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="yellow_taxi_raw_batch"
    )

    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": dataframe}
    )

    expectation_suite = gx.ExpectationSuite(
        name="yellow_taxi_raw_suite"
    )

    expectation_suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=1,
        )
    )

    expectation_suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=EXPECTED_COLUMNS,
        )
    )

    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="tpep_pickup_datetime",
        )
    )

    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="tpep_dropoff_datetime",
        )
    )

    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="PULocationID",
            min_value=1,
            max_value=265,
        )
    )

    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="DOLocationID",
            min_value=1,
            max_value=265,
        )
    )

    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="trip_distance",
            min_value=0,
            mostly=0.999,
        )
    )

    validation_result = batch.validate(
        expectation_suite
    )

    print("\n=== RÉSULTAT GREAT EXPECTATIONS ===")
    print(f"Succès global : {validation_result.success}")

    for result in validation_result.results:
        expectation_type = result.expectation_config.type
        success = result.success
        unexpected_count = result.result.get("unexpected_count", 0)

        print(
            f"- {expectation_type}: "
            f"success={success}, "
            f"unexpected_count={unexpected_count}"
        )

    return bool(validation_result.success)


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(
        description="Valider un fichier NYC Taxi brut."
    )

    parser.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Chemin du fichier Parquet.",
    )

    return parser.parse_args()


def main() -> int:
    """Run validation."""

    arguments = parse_arguments()

    try:
        success = validate_raw_file(arguments.file)
        return 0 if success else 1

    except Exception as error:
        print(f"Erreur de validation : {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())