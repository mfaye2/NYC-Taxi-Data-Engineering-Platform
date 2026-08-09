from datetime import datetime, timedelta
import time
import subprocess
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator


AWS_PROFILE = "nyc-taxi-dev"
AWS_REGION = "eu-north-1"

GLUE_JOB_NAME = "nyc-taxi-data-platform-dev-transform-trips"

CURATED_CRAWLER = "nyc-taxi-data-platform-dev-curated-trips-crawler"
REJECTED_CRAWLER = "nyc-taxi-data-platform-dev-rejected-trips-crawler"

S3_BUCKET = "nyc-taxi-data-platform-dev-98713520"


# ---------------------------------------------------------
# AWS CLIENT
# ---------------------------------------------------------

def get_glue_client():
    session = boto3.Session(
        profile_name=AWS_PROFILE,
        region_name=AWS_REGION
    )

    return session.client("glue")


# ---------------------------------------------------------
# INGESTION
# ---------------------------------------------------------
def get_processing_dates(context):
    start_date_str = context["params"]["start_date"]
    end_date_str = context["params"]["end_date"]

    start_date = datetime.strptime(
        start_date_str,
        "%d/%m/%Y"
    )

    end_date = datetime.strptime(
        end_date_str,
        "%d/%m/%Y"
    )

    if end_date < start_date:
        raise ValueError(
            "end_date doit être supérieure ou égale à start_date"
        )

    return start_date, end_date



def get_months_between(start_date, end_date):
    months = []

    current_year = start_date.year
    current_month = start_date.month

    while (
        current_year < end_date.year
        or (
            current_year == end_date.year
            and current_month <= end_date.month
        )
    ):
        months.append(
            (current_year, current_month)
        )

        if current_month == 12:
            current_month = 1
            current_year += 1
        else:
            current_month += 1

    return months



def download_month(**context):
    start_date, end_date = get_processing_dates(context)

    months = get_months_between(
        start_date,
        end_date
    )

    for year, month in months:

        print(
            f"Téléchargement du mois : "
            f"{month:02d}/{year}"
        )

        command = (
            "cd /opt/project && "
            "python src/extraction/download_trips.py "
            f"--year {year} "
            f"--month {month}"
        )

        run_command(command)


def upload_month_to_s3(**context):
    start_date, end_date = get_processing_dates(context)

    months = get_months_between(
        start_date,
        end_date
    )

    session = boto3.Session(
        profile_name=AWS_PROFILE,
        region_name=AWS_REGION
    )

    s3 = session.client("s3")

    for year, month in months:

        month_str = f"{month:02d}"

        local_file = (
            f"/opt/project/data/raw/trips/"
            f"year={year}/"
            f"month={month_str}/"
            f"yellow_tripdata_{year}-{month_str}.parquet"
        )

        s3_key = (
            f"raw/trips/"
            f"year={year}/"
            f"month={month_str}/"
            f"yellow_tripdata_{year}-{month_str}.parquet"
        )

        print(
            f"Upload : {local_file}"
        )

        print(
            f"Vers : s3://{S3_BUCKET}/{s3_key}"
        )

        s3.upload_file(
            local_file,
            S3_BUCKET,
            s3_key
        )

    print("Tous les mois ont été envoyés vers S3.")


# ---------------------------------------------------------
# GLUE JOB
# ---------------------------------------------------------

def start_glue_job(**context):
    glue = get_glue_client()

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME
    )

    job_run_id = response["JobRunId"]

    print(f"Glue JobRunId: {job_run_id}")

    context["ti"].xcom_push(
        key="glue_job_run_id",
        value=job_run_id
    )


def wait_for_glue_job(**context):
    glue = get_glue_client()

    job_run_id = context["ti"].xcom_pull(
        task_ids="start_glue_job",
        key="glue_job_run_id"
    )

    while True:
        response = glue.get_job_run(
            JobName=GLUE_JOB_NAME,
            RunId=job_run_id,
            PredecessorsIncluded=False
        )

        state = response["JobRun"]["JobRunState"]

        print(f"Glue status: {state}")

        if state == "SUCCEEDED":
            print("Glue job terminé avec succès.")
            break

        if state in [
            "FAILED",
            "STOPPED",
            "TIMEOUT",
            "ERROR",
            "EXPIRED"
        ]:
            raise RuntimeError(
                f"Glue job terminé avec l'état : {state}"
            )

        time.sleep(30)


# ---------------------------------------------------------
# CRAWLERS
# ---------------------------------------------------------

def start_crawler(crawler_name):
    glue = get_glue_client()

    glue.start_crawler(
        Name=crawler_name
    )

    print(f"Crawler démarré : {crawler_name}")


def wait_for_crawler(crawler_name):
    glue = get_glue_client()

    while True:
        response = glue.get_crawler(
            Name=crawler_name
        )

        state = response["Crawler"]["State"]

        print(f"{crawler_name} status: {state}")

        if state == "READY":
            print(f"{crawler_name} terminé.")
            break

        time.sleep(20)


def start_curated_crawler():
    start_crawler(CURATED_CRAWLER)


def wait_curated_crawler():
    wait_for_crawler(CURATED_CRAWLER)


def start_rejected_crawler():
    start_crawler(REJECTED_CRAWLER)


def wait_rejected_crawler():
    wait_for_crawler(REJECTED_CRAWLER)


# ---------------------------------------------------------
# DBT
# ---------------------------------------------------------

def run_command(command: str):
    print(f"Commande exécutée : {command}")

    result = subprocess.run(
        command,
        shell=True,
        text=True,
        capture_output=True
    )

    print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Commande échouée avec le code {result.returncode}"
        )


def run_dbt():
    run_command(
        "cd /opt/project/dbt && "
        "dbt run --no-partial-parse --profiles-dir ."
    )


def test_dbt():
    run_command(
        "cd /opt/project/dbt && "
        "dbt test --profiles-dir ."
    )


# ---------------------------------------------------------
# AIRFLOW CONFIG
# ---------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="nyc_taxi_data_pipeline",
    description="NYC Taxi AWS data engineering pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={
    "start_date": "25/03/2025",
    "end_date": "31/03/2025",
    },
    tags=["nyc-taxi", "aws", "glue", "dbt"],
) as dag:

    download_trips = PythonOperator(
        task_id="download_month",
        python_callable=download_month,
    )

    upload_trips = PythonOperator(
        task_id="upload_month_to_s3",
        python_callable=upload_month_to_s3,
    )
    
    start_glue = PythonOperator(
        task_id="start_glue_job",
        python_callable=start_glue_job,
    )

    wait_glue = PythonOperator(
        task_id="wait_for_glue_job",
        python_callable=wait_for_glue_job,
    )

    curated_crawler = PythonOperator(
        task_id="start_curated_crawler",
        python_callable=start_curated_crawler,
    )

    wait_curated = PythonOperator(
        task_id="wait_curated_crawler",
        python_callable=wait_curated_crawler,
    )

    rejected_crawler = PythonOperator(
        task_id="start_rejected_crawler",
        python_callable=start_rejected_crawler,
    )

    wait_rejected = PythonOperator(
        task_id="wait_rejected_crawler",
        python_callable=wait_rejected_crawler,
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt,
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=test_dbt,
    )


    (
        download_trips
        >> upload_trips
        >> start_glue
        >> wait_glue
        >> curated_crawler
        >> wait_curated
        >> rejected_crawler
        >> wait_rejected
        >> dbt_run
        >> dbt_test
    )