"""Inspect a local NYC Yellow Taxi Parquet file."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def inspect_parquet(file_path: Path) -> None:
    """Display basic information about a Parquet dataset."""
    if not file_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {file_path}")

    dataframe = pd.read_parquet(file_path)

    print("\n=== INFORMATIONS GÉNÉRALES ===")
    print(f"Fichier : {file_path}")
    print(f"Nombre de lignes : {len(dataframe):,}")
    print(f"Nombre de colonnes : {len(dataframe.columns)}")
    print(f"Taille du fichier : {file_path.stat().st_size / (1024 ** 2):.2f} Mo")

    print("\n=== COLONNES ===")
    for column in dataframe.columns:
        print(f"- {column}: {dataframe[column].dtype}")

    print("\n=== CINQ PREMIÈRES LIGNES ===")
    print(dataframe.head())

    print("\n=== VALEURS NULLES ===")
    null_values = dataframe.isna().sum().sort_values(ascending=False)
    print(null_values[null_values > 0])

    print("\n=== STATISTIQUES NUMÉRIQUES ===")
    print(dataframe.describe().transpose())


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Explorer un fichier NYC Taxi au format Parquet."
    )

    parser.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Chemin du fichier Parquet.",
    )

    return parser.parse_args()


def main() -> None:
    """Run the inspection."""
    arguments = parse_arguments()
    inspect_parquet(arguments.file)


if __name__ == "__main__":
    main()