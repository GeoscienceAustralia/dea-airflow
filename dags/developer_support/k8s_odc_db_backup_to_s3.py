"""
# odc database in RDS backup and store to s3

DAG to periodically backup ODC database data.

This DAG uses k8s executors and in cluster with relevant tooling
and configuration installed.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from textwrap import dedent

from env_var.infra import (
    NODE_AFFINITY,
    SECRET_DBA_ADMIN_NAME,
    DB_DUMP_S3_ROLE,
    SECRET_AWS_NAME,
    DB_DUMP_S3_BUCKET,
    DB_DATABASE,
    DB_HOSTNAME,
)

DAG_NAME = "odc_db_dump_to_s3"

S3_TO_RDS_IMAGE = "geoscienceaustralia/s3-to-rds:latest"


# DAG CONFIGURATION
DEFAULT_ARGS = {
    "owner": "Pin Jin",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["pin.jin@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        # TODO: Pass these via templated params in DAG Run
        "DB_HOSTNAME": DB_HOSTNAME,
        "DB_DATABASE": DB_DATABASE,
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", SECRET_DBA_ADMIN_NAME, "postgres-username"),
        Secret("env", "PGPASSWORD", SECRET_DBA_ADMIN_NAME, "postgres-password"),
        Secret("env", "AWS_DEFAULT_REGION", SECRET_AWS_NAME, "AWS_DEFAULT_REGION"),
    ],
}

TEST_COMMAND = [
    "bash",
    "-c",
    dedent(
        """
            psql -h $(DB_HOSTNAME) -U $(DB_USERNAME) -d $(DB_DATABASE) -t -A -F"," -c "select count(*) from cubedash.product;" > output.csv
            aws s3 ls s3://%s
        """
    )
    % (DB_DUMP_S3_BUCKET),
]

# THE DAG
dag = DAG(
    dag_id=DAG_NAME,
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval="@weekly",  # weekly
    catchup=False,
    tags=["k8s", "developer_support", "rds", "s3", "db"],
)

with dag:
    # DB_DUMP = KubernetesPodOperator(
    #     namespace="processing",
    #     image=INDEXER_IMAGE,
    #     cmds=["pg_dump"],
    #     arguments=[
    #         "-h",
    #         "$(DB_HOSTNAME)",
    #         "-U",
    #         "$(DB_USERNAME)",
    #         "-d",
    #         "$(DB_DATABASE)",
    #         "-d",
    #         "$(DB_DATABASE)",
    #     ],
    #     labels={"step": "ds-arch"},
    #     name="datacube-dataset-archive",
    #     task_id="archive-nrt-datasets",
    #     get_logs=True,
    #     affinity=NODE_AFFINITY,
    #     is_delete_operator_pod=True,
    # )
    DB_DUMP_TEST = KubernetesPodOperator(
        namespace="processing",
        image=S3_TO_RDS_IMAGE,
        arguments=TEST_COMMAND,
        labels={"step": "ds-arch"},
        name="dump-odc-db",
        task_id="dump-odc-db",
        get_logs=True,
        affinity=NODE_AFFINITY,
        is_delete_operator_pod=True,
    )
