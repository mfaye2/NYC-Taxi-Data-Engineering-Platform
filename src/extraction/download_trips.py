"""Download monthly NYC Yellow Taxi trip data.

Example:
    python src/extraction/download_trips.py --year 2025 --month 1
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import requests


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DEFAULT_OUTPUT_DIRECTORY = Path("data/raw/trips")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

LOGGER = logging.getLogger(__name__)


def build_file_name(year: int, month: int) -> str:
    """Build the official NYC Yellow Taxi file name."""
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def build_download_url(year: int, month: int) -> str:
    """Build the official download URL."""
    file_name = build_file_name(year, month)
    return f"{BASE_URL}/{file_name}"


def validate_period(year: int, month: int) -> None:
    """Validate the requested year and month."""
    if year < 2009:
        raise ValueError("L'année doit être supérieure ou égale à 2009.")

    if month < 1 or month > 12:
        raise ValueError("Le mois doit être compris entre 1 et 12.")


def download_file(
    year: int,
    month: int,
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY,
    overwrite: bool = False,
) -> Path:
    """Download one monthly Yellow Taxi Parquet file."""
    validate_period(year, month)

    file_name = build_file_name(year, month)
    download_url = build_download_url(year, month)

    destination_directory = (
        output_directory / f"year={year}" / f"month={month:02d}"
    )
    destination_directory.mkdir(parents=True, exist_ok=True)

    destination_path = destination_directory / file_name

    if destination_path.exists() and not overwrite:
        LOGGER.info("Le fichier existe déjà : %s", destination_path)
        return destination_path

    LOGGER.info("Téléchargement depuis : %s", download_url)

    temporary_path = destination_path.with_suffix(".parquet.part")

    try:
        with requests.get(
            download_url,
            stream=True,
            timeout=(10, 120),
        ) as response:
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "")
            content_length = response.headers.get("Content-Length")

            LOGGER.info("Type de contenu : %s", content_type or "inconnu")
            LOGGER.info(
                "Taille annoncée : %s octets",
                content_length or "inconnue",
            )

            with temporary_path.open("wb") as output_file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        output_file.write(chunk)

        if not temporary_path.exists() or temporary_path.stat().st_size == 0:
            raise RuntimeError("Le fichier téléchargé est vide.")

        temporary_path.replace(destination_path)

        LOGGER.info(
            "Téléchargement terminé : %s (%.2f Mo)",
            destination_path,
            destination_path.stat().st_size / (1024 * 1024),
        )

        return destination_path

    except requests.RequestException as error:
        if temporary_path.exists():
            temporary_path.unlink()

        raise RuntimeError(
            f"Échec du téléchargement depuis {download_url}"
        ) from error

    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()
        raise


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Télécharger un fichier mensuel NYC Yellow Taxi."
    )

    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Année à télécharger, par exemple 2025.",
    )

    parser.add_argument(
        "--month",
        type=int,
        required=True,
        help="Mois à télécharger, entre 1 et 12.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Remplacer le fichier local s'il existe déjà.",
    )

    return parser.parse_args()


def main() -> int:
    """Run the command-line program."""
    arguments = parse_arguments()

    try:
        downloaded_file = download_file(
            year=arguments.year,
            month=arguments.month,
            overwrite=arguments.overwrite,
        )

        LOGGER.info("Fichier disponible : %s", downloaded_file)
        return 0

    except (ValueError, RuntimeError) as error:
        LOGGER.error("%s", error)
        return 1


if __name__ == "__main__":
    sys.exit(main())