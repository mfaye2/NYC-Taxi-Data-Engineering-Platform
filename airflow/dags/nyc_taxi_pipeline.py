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
    tags=["nyc-taxi", "aws", "glue", "dbt"],
) as dag:

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
        start_glue
        >> wait_glue
        >> curated_crawler
        >> wait_curated
        >> rejected_crawler
        >> wait_rejected
        >> dbt_run
        >> dbt_test
    )