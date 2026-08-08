from datetime import datetime, timedelta
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator


AWS_PROFILE = "nyc-taxi-dev"
AWS_REGION = "eu-north-1"

GLUE_JOB_NAME = "nyc-taxi-data-platform-dev-transform-trips"

CURATED_CRAWLER = "nyc-taxi-data-platform-dev-curated-trips-crawler"
REJECTED_CRAWLER = "nyc-taxi-data-platform-dev-rejected-trips-crawler"


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
            f"La commande a échoué avec le code {result.returncode}"
        )


def start_glue_job():
    command = (
        f"aws glue start-job-run "
        f'--job-name "{GLUE_JOB_NAME}" '
        f"--profile {AWS_PROFILE} "
        f"--region {AWS_REGION}"
    )

    run_command(command)


def run_curated_crawler():
    command = (
        f"aws glue start-crawler "
        f'--name "{CURATED_CRAWLER}" '
        f"--profile {AWS_PROFILE} "
        f"--region {AWS_REGION}"
    )

    run_command(command)


def run_rejected_crawler():
    command = (
        f"aws glue start-crawler "
        f'--name "{REJECTED_CRAWLER}" '
        f"--profile {AWS_PROFILE} "
        f"--region {AWS_REGION}"
    )

    run_command(command)


def run_dbt():
    command = (
        "cd /opt/project/dbt && "
        "dbt run --profiles-dir ."
    )

    run_command(command)


def test_dbt():
    command = (
        "cd /opt/project/dbt && "
        "dbt test --profiles-dir ."
    )

    run_command(command)


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

    curated_crawler = PythonOperator(
        task_id="run_curated_crawler",
        python_callable=run_curated_crawler,
    )

    rejected_crawler = PythonOperator(
        task_id="run_rejected_crawler",
        python_callable=run_rejected_crawler,
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt,
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=test_dbt,
    )

    start_glue >> curated_crawler >> rejected_crawler >> dbt_run >> dbt_test