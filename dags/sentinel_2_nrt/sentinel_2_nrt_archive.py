"""
# Sentinel-2_nrt Archiving automation

DAG to periodically archive Sentinel-2 NRT data.

This DAG uses k8s executors and in cluster with relevant tooling
and configuration installed.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

from textwrap import dedent

import kubernetes.client.models as k8s

from sentinel_2_nrt.images import INDEXER_IMAGE
from sentinel_2_nrt.ows_views import OWS_UPDATE_EXTENTS
from sentinel_2_nrt.env_cfg import DB_DATABASE, SECRET_OWS_NAME, SECRET_AWS_NAME


ARCHIVE_CONDITION = "[$(date -d '-365 day' +%F), $(date -d '-91 day' +%F)]"
ARCHIVE_PRODUCTS = "s2a_nrt_granule s2b_nrt_granule"

ARCHIVE_BASH_COMMAND = [
    "bash",
    "-c",
    dedent(
        """
        for product in %s; do
            echo "Archiving product: $product"
            datacube dataset search -f csv "product=$product time in %s" > /tmp/to_kill.csv;
            cat /tmp/to_kill.csv | awk -F',' '{print $1}' | sed '1d' > /tmp/to_kill.list;
            echo "Datasets count to be archived"
            wc -l /tmp/to_kill.list;
            cat /tmp/to_kill.list | xargs datacube dataset archive
        done;
    """
    )
    % (ARCHIVE_PRODUCTS, ARCHIVE_CONDITION),
]

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
        "DB_HOSTNAME": "db-writer",
        "DB_DATABASE": DB_DATABASE,
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", SECRET_OWS_NAME, "postgres-username"),
        Secret("env", "DB_PASSWORD", SECRET_OWS_NAME, "postgres-password"),
        Secret("env", "AWS_DEFAULT_REGION", SECRET_AWS_NAME, "AWS_DEFAULT_REGION"),
    ],
}

# THE DAG
dag = DAG(
    "sentinel_2_nrt_archive",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */1 * * *",
    catchup=False,
    tags=["k8s", "sentinel-2"],
)

with dag:
    OWS_UPDATE_EXTENT_AFTER_ARCHIVE = OWS_UPDATE_EXTENTS
    ARCHIVE_EXTRANEOUS_DS = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        arguments=ARCHIVE_BASH_COMMAND,
        labels={"step": "ds-arch"},
        name="datacube-dataset-archive",
        task_id="archive-nrt-datasets",
        get_logs=True,
        is_delete_operator_pod=True,
    )

    START = DummyOperator(task_id="start_sentinel_2_nrt")

    COMPLETE = DummyOperator(task_id="all_done")

    START >> ARCHIVE_EXTRANEOUS_DS
    ARCHIVE_EXTRANEOUS_DS >> OWS_UPDATE_EXTENT_AFTER_ARCHIVE
    OWS_UPDATE_EXTENT_AFTER_ARCHIVE >> COMPLETE