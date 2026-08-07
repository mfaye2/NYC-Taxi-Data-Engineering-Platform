"""Upload NYC Taxi files to Amazon S3.

Examples:
    python src/extraction/upload_to_s3.py \
        --file "data/raw/trips/year=2025/month=01/yellow_tripdata_2025-01.parquet" \
        --bucket "nyc-taxi-data-platform-dev-98713520" \
        --key "raw/trips/year=2025/month=01/yellow_tripdata_2025-01.parquet"

    python src/extraction/upload_to_s3.py \
        --file "data/raw/zones/taxi_zone_lookup.csv" \
        --bucket "nyc-taxi-data-platform-dev-98713520" \
        --key "raw/zones/taxi_zone_lookup.csv"
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError


LOGGER = logging.getLogger(__name__)


def object_exists(
    s3_client,
    bucket_name: str,
    object_key: str,
) -> bool:
    """Return True if the object already exists in S3."""

    try:
        s3_client.head_object(
            Bucket=bucket_name,
            Key=object_key,
        )
        return True

    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")

        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return False

        raise


def upload_file(
    file_path: Path,
    bucket_name: str,
    object_key: str,
    profile_name: str,
    overwrite: bool = False,
) -> None:
    """Upload one local file to Amazon S3."""

    if not file_path.exists():
        raise FileNotFoundError(
            f"Fichier local introuvable : {file_path}"
        )

    if not file_path.is_file():
        raise ValueError(
            f"Le chemin n'est pas un fichier : {file_path}"
        )

    session = boto3.Session(
        profile_name=profile_name,
    )

    s3_client = session.client("s3")

    if object_exists(
        s3_client=s3_client,
        bucket_name=bucket_name,
        object_key=object_key,
    ):
        if not overwrite:
            LOGGER.info(
                "L'objet existe déjà dans S3 : s3://%s/%s",
                bucket_name,
                object_key,
            )
            return

        LOGGER.warning(
            "L'objet existe déjà et sera remplacé : s3://%s/%s",
            bucket_name,
            object_key,
        )

    LOGGER.info(
        "Upload de %s vers s3://%s/%s",
        file_path,
        bucket_name,
        object_key,
    )

    try:
        s3_client.upload_file(
            Filename=str(file_path),
            Bucket=bucket_name,
            Key=object_key,
        )

    except (BotoCoreError, ClientError) as error:
        raise RuntimeError(
            f"Échec de l'upload vers s3://{bucket_name}/{object_key}"
        ) from error

    LOGGER.info("Upload terminé avec succès.")


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(
        description="Uploader un fichier local vers Amazon S3."
    )

    parser.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Chemin du fichier local.",
    )

    parser.add_argument(
        "--bucket",
        required=True,
        help="Nom du bucket S3.",
    )

    parser.add_argument(
        "--key",
        required=True,
        help="Chemin de l'objet dans S3.",
    )

    parser.add_argument(
        "--profile",
        default="nyc-taxi-dev",
        help="Profil AWS CLI à utiliser.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Remplacer l'objet s'il existe déjà.",
    )

    return parser.parse_args()


def main() -> int:
    """Run the upload command."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    arguments = parse_arguments()

    try:
        upload_file(
            file_path=arguments.file,
            bucket_name=arguments.bucket,
            object_key=arguments.key,
            profile_name=arguments.profile,
            overwrite=arguments.overwrite,
        )
        return 0

    except (
        FileNotFoundError,
        ValueError,
        RuntimeError,
        ClientError,
        BotoCoreError,
    ) as error:
        LOGGER.error("%s", error)
        return 1


if __name__ == "__main__":
    sys.exit(main())