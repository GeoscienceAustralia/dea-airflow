"""
Run WAGL NRT pipeline in Airflow.
"""
import os
from datetime import datetime, timedelta
import csv
from pathlib import Path
import json

from airflow import DAG
from airflow import configuration

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.contrib.sensors.aws_sqs_sensor import SQSSensor

default_args = {
    "owner": "Imam Alam",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 11),
    "email": ["imam.alam@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

WAGL_IMAGE = "451924316694.dkr.ecr.ap-southeast-2.amazonaws.com/dev/wagl:rc-20190109-5"

TILE_LIST = "assets/S2_aoi.csv"

COPY_SCENE_QUEUE = "https://sqs.ap-southeast-2.amazonaws.com/451924316694/dea-dev-eks-wagl-s2-nrt-copy-scene"


def australia_region_codes():
    root = Path(configuration.get("core", "dags_folder")).parent

    with open(root / TILE_LIST) as fl:
        reader = csv.reader(fl)
        return {x[0] for x in reader}


def region_code(message):
    msg_dict = json.loads(message)
    tiles = msg_dict["tiles"]
    assert len(tiles) == 0
    tile = tiles[0]
    return str(tile["utmZone"]) + tile["latitudeBand"] + tile["gridSquare"]


def test_env(**kwargs):
    for key, value in kwargs.items():
        print("kwargs key:", key)
    for key, value in os.environ.items():
        print("env key:", key)
    print(australia_region_codes())


pipeline = DAG(
    "k8s_wagl_nrt",
    doc_md=__doc__,
    default_args=default_args,
    description="DEA Sentinel-2 NRT Processing",
    concurrency=2,
    max_active_runs=1,
    catchup=False,
    params={},
    schedule_interval=None,
    tags=["k8s", "dea", "psc", "wagl", "nrt"],
)

with pipeline:
    START = DummyOperator(task_id="start_wagl")

    ENV = PythonOperator(
        task_id="test_env",
        python_callable=test_env,
        provide_context=True,
    )

    SENSOR = SQSSensor(
        task_id="copy_scene_queue_sensor",
        sqs_queue=COPY_SCENE_QUEUE,
        region_name="ap-southeast-2",
    )

    WAGL_RUN = KubernetesPodOperator(
        namespace="processing",
        name="dea-s2-wagl-nrt",
        task_id="dea-s2-wagl-nrt",
        image_pull_policy="IfNotPresent",
        image=WAGL_IMAGE,
        is_delete_operator_pod=True,
        arguments=["--version"],
        labels={"runner": "airflow"},
        get_logs=True,
    )

    END = DummyOperator(task_id="end_wagl")

    START >> ENV >> SENSOR >> WAGL_RUN >> END
