"""
# S3 to Datacube Indexing

DAG to periodically/one-shot update explorer and ows schemas in RDS
after a given Dataset has been indexed from S3.

- Run Explorer summaries
- Run ows update ranges for NRT products
- Run ows update ranges for NRT multi-products

This DAG uses k8s executors and pre-existing pods in cluster with relevant tooling
and configuration installed.

The DAG has to be parameterized with S3_Glob and Target product as below.

    "s3_glob": "s3://dea-public-data/cemp_insar/insar/displacement/alos//**/*.yaml",
    "product": "cemp_insar_alos_displacement"

"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.operators.dummy_operator import DummyOperator
import kubernetes.client.models as k8s
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.kubernetes.volume import Volume

DEFAULT_ARGS = {
    "owner": "Pin Jin",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 4),
    "email": ["pin.jin@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        "AWS_DEFAULT_REGION": "ap-southeast-2",
        # TODO: Pass these via templated params in DAG Run
        "DB_HOSTNAME": "database-write.local",
        "DB_DATABASE": "ows-index",
        "WMS_CONFIG_PATH": "/env/config/ows_cfg.py",
        "DATACUBE_OWS_CFG": "config.ows_cfg.ows_cfg"
    },
    # Use K8S secrets to send DB Creds
    # Lift secrets into environment variables for datacube
    "secrets": [
        Secret("env", "DB_USERNAME", "ows-db", "postgres-username"),
        Secret("env", "DB_PASSWORD", "ows-db", "postgres-password"),
    ],
}

OWS_IMAGE = "opendatacube/ows:1.8.1"
OWS_CONFIG_IMAGE = "geoscienceaustralia/dea-datakube-config:1.5.1"

OWS_CFG_PATH = "/env/config/ows_cfg.py"
OWS_CFG_IMAGEPATH = "/opt/dea-config/dev/services/wms/ows/ows_cfg.py"

cfg_image_mount = k8s.V1VolumeMount(
      mount_path='/opt',
      name='cfg-image-volume',
      sub_path=None,
      read_only=True
)

# ows_cfg_mount = VolumeMount('ows-config-volume',
#                             mount_path='/env/config',
#                             sub_path=None,
#                             read_only=True)

# ows_cfg_volume_config= {
#     'ConfigMap':
#         {
#             'claimName': 'ows-config-volume'
#         }
# }

# ows_cfg_volume = Volume(name='ows-config-volume', configs=ows_cfg_volume_config)


config_container = k8s.V1Container(
        image=OWS_CONFIG_IMAGE,
        command=["ls"],
        args=["/opt"],
        volume_mounts=[cfg_image_mount],
        name="mount-ows-config",
    )
dag = DAG(
    "k8s_ows_pod_pin",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["k8s"]
)


with dag:
    START = DummyOperator(task_id="s3_index_publish")

    UPDATE_RANGES = KubernetesPodOperator(
        namespace="processing",
        image=OWS_IMAGE,
        cmds=["ls"],
        arguments=["/opt"],
        labels={"step": "ows"},
        name="ows-update-ranges",
        task_id="update-ranges-task",
        get_logs=True,
        # volumes=[ows_cfg_volume],
        # volume_mounts=[ows_cfg_mount],
        init_containers=[config_container]
    )

    COMPLETE = DummyOperator(task_id="all_done")

    # START >> BOOTSTRAP
    # BOOTSTRAP >> INDEXING
    # INDEXING >> UPDATE_RANGES
    # INDEXING >> SUMMARY
    # UPDATE_RANGES >> COMPLETE
    # SUMMARY >> COMPLETE
    START >> UPDATE_RANGES
    UPDATE_RANGES >> COMPLETE