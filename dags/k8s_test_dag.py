from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.pod import Resources

from unittest.mock import patch

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

pipeline = DAG(
    "k8s_test_dag",
    doc_md=__doc__,
    default_args=default_args,
    description="test dag please ignore",
    catchup=False,
    params={},
    schedule_interval=None,  # timedelta(minutes=30),
    tags=["k8s", "dea", "psc", "wagl", "nrt"],
)

resources = {
    "request_memory": "2G",
    "request_cpu": "1000m",
}

with pipeline:
    with patch.object(
        Resources, "to_k8s_client_obj", side_effect=KeyError
    ) as mock_resources:
        res = Resources(**resources)
        val = res.to_k8s_client_obj()

        COPY = KubernetesPodOperator(
            namespace="processing",
            name="test_dag",
            task_id="test_dag",
            image_pull_policy="IfNotPresent",
            image="ubuntu:18.04",
            cmds=["echo", "test dag please ignore", f"{val}"],
            labels={
                "runner": "airflow",
                "product": "Sentinel-2",
                "app": "nrt",
                "stage": "test",
            },
            get_logs=True,
            is_delete_operator_pod=True,
        )
