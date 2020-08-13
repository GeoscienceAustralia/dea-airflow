"""
# Landsat Collection-3 indexing automation

DAG to periodically index/archive Landsat Collection-3 data.

This DAG uses k8s executors and in cluster with relevant tooling
and configuration installed.

"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


DEFAULT_ARGS = {
    "owner": "Sachit Rajbhandari",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 1),
    "email": ["sachit.rajbhandari@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "aws_conn_id": "dea_public_data_upload",
    "add_sqs_queue": "dea-dev-eks-landsat-c3-add",
    "archive_sqs_queue": "dea-dev-eks-landsat-c3-archive",
    "products": "ga_ls5t_ard_3 ga_ls7e_ard_3 ga_ls8c_ard_3",
    "env_vars": {
        # TODO: Change to dev DB credentials
        "DB_HOSTNAME": "database-write.local",
        "DB_DATABASE": "ows-index",
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", "ows-db", "postgres-username"),
        Secret("env", "DB_PASSWORD", "ows-db", "postgres-password"),
        Secret("env", "AWS_DEFAULT_REGION", "indexing-aws-creds-dev", "AWS_DEFAULT_REGION"),
        Secret("env", "AWS_ACCESS_KEY_ID", "indexing-aws-creds-dev", "AWS_ACCESS_KEY_ID"),
        Secret("env", "AWS_SECRET_ACCESS_KEY", "indexing-aws-creds-dev", "AWS_SECRET_ACCESS_KEY"),
    ],
}

# TODO: Need to change to new image
INDEXER_IMAGE = "opendatacube/datacube-index:0.0.8"


dag = DAG(
    "k8s_index_ls_c3",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # TODO: Schedule '0 */1 * * *',
    catchup=False,
    tags=["k8s", "landsat_c3"]
)

with dag:

    INDEXING = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        image_pull_policy='Always',
        arguments=["sqs-to-dc",
                   "--odc-metadata-link",
                   "--skip-lineage",
                   dag.default_args['add_sqs_queue'],
                   dag.default_args['products']],
        labels={"step": "sqs-dc-indexing"},
        name="datacube-index",
        task_id="indexing-task",
        get_logs=True,
        is_delete_operator_pod=True,
    )

    ARCHIVING = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        image_pull_policy='Always',
        arguments=["sqs-to-dc",
                   "--odc-metadata-link",
                   "--archive",
                   dag.default_args['archive_sqs_queue'],
                   dag.default_args['products']],
        labels={"step": "sqs-dc-archiving"},
        name="datacube-archive",
        task_id="archiving-task",
        get_logs=True,
        is_delete_operator_pod=True,
    )

    INDEXING >> ARCHIVING
