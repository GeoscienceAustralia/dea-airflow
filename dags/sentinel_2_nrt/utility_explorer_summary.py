"""
## Utility Tool
### explore refresh stats
This is utility is to provide administrators the easy accessiblity to run ad-hoc --refresh-stats

#### default run
    `cubedash-gen --no-init-database --refresh-stats --force-refresh s2a_nrt_granule`
    `cubedash-gen --no-init-database --refresh-stats --force-refresh s2b_nrt_granule`

#### Utility customisation
The DAG can be parameterized with run time configuration `products`

To run with all, set `dag_run.conf.products` to `--all`
otherwise provide products to be refreshed seperated by space, i.e. `s2a_nrt_granule s2b_nrt_granule`
dag_run.conf format:
    `"products": "--all"`
    `"products": "s2a_nrt_granule s2b_nrt_granule"`
"""

from airflow import DAG
from textwrap import dedent
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.operators.subdag_operator import SubDagOperator
from sentinel_2_nrt.subdag_explorer_summary import explorer_refresh_stats_subdag
from sentinel_2_nrt.env_cfg import INDEXING_PRODUCTS, DB_DATABASE, SECRET_EXPLORER_NAME, SECRET_AWS_NAME
from sentinel_2_nrt.test_subdag import subdag_test

DAG_NAME = "utility_explorer-refresh-stats"

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
        Secret("env", "AWS_DEFAULT_REGION", SECRET_AWS_NAME, "AWS_DEFAULT_REGION"),
    ],
}


# THE DAG
dag = DAG(
    dag_id=DAG_NAME,
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */1 * * *",
    catchup=False,
    tags=["k8s", "explorer"],
)

def parse_dagrun_conf(products, **kwargs):
    if products:
        return products
    else:
        return INDEXING_PRODUCTS

with dag:

    SET_PRODUCTS = PythonOperator(
        task_id='parse_dagrun_conf',
        python_callable=parse_dagrun_conf,
        op_args=["{{ dag_run.conf.products }}"],
        provide_context=True,
    )

    t2 = SubDagOperator(
        task_id="test_sub_dag",
        subdag=subdag_test(DAG_NAME, "test_sub_dag", DEFAULT_ARGS, "{{ ti.xcom_pull(task_ids='parse_dagrun_conf', key='return_value') }}")
    )


    EXPLORER_SUMMARY = SubDagOperator(
        task_id="run-cubedash-gen-refresh-stat",
        subdag=explorer_refresh_stats_subdag(DAG_NAME, "run-cubedash-gen-refresh-stat", DEFAULT_ARGS, "{{ ti.xcom_pull(task_ids='parse_dagrun_conf') }}"),
    )

    START = DummyOperator(task_id="start_explorer_refresh_stats")

    COMPLETE = DummyOperator(task_id="all_done")

    START >> SET_PRODUCTS
    SET_PRODUCTS >> t2
    # SET_PRODUCTS >> EXPLORER_SUMMARY
    # EXPLORER_SUMMARY >> COMPLETE
