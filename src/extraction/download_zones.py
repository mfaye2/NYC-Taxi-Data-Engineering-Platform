"""Download the official NYC Taxi Zone Lookup table."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import requests


ZONE_LOOKUP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
)

OUTPUT_PATH = Path("data/raw/zones/taxi_zone_lookup.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

LOGGER = logging.getLogger(__name__)


def download_zone_lookup(
    output_path: Path = OUTPUT_PATH,
    overwrite: bool = False,
) -> Path:
    """Download the official NYC Taxi Zone Lookup CSV file."""

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not overwrite:
        LOGGER.info("Le fichier existe déjà : %s", output_path)
        return output_path

    temporary_path = output_path.with_suffix(".csv.part")

    try:
        LOGGER.info("Téléchargement depuis : %s", ZONE_LOOKUP_URL)

        with requests.get(
            ZONE_LOOKUP_URL,
            stream=True,
            timeout=(10, 60),
        ) as response:
            response.raise_for_status()

            with temporary_path.open("wb") as output_file:
                for chunk in response.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        output_file.write(chunk)

        if not temporary_path.exists() or temporary_path.stat().st_size == 0:
            raise RuntimeError("Le fichier des zones téléchargé est vide.")

        temporary_path.replace(output_path)

        LOGGER.info(
            "Téléchargement terminé : %s (%.2f Ko)",
            output_path,
            output_path.stat().st_size / 1024,
        )

        return output_path

    except requests.RequestException as error:
        if temporary_path.exists():
            temporary_path.unlink()

        raise RuntimeError(
            "Échec du téléchargement du référentiel des zones."
        ) from error

    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()
        raise


def main() -> int:
    """Run the download command."""

    try:
        downloaded_path = download_zone_lookup()
        LOGGER.info("Fichier disponible : %s", downloaded_path)
        return 0

    except RuntimeError as error:
        LOGGER.error("%s", error)
        return 1


if __name__ == "__main__":
    sys.exit(main())