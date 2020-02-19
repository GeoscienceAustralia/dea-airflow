from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Damien Ayers',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 1),
    'email': ['damien.ayers@ga.gov.au'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    'params': {'project': 'v10',
               'queue': 'normal',
               'module': 'dea/unstable',
               'year': '2019'
               }
}

synced_products = ['ls8_nbar_scene',
                   'ls8_nbart_scene',
                   'ls8_pq_scene',
                   'ls8_pq_legacy_scene']
unsynced = 'ls7_nbar_scene,ls7_nbart_scene,ls7_pq_scene,ls7_pq_legacy_scene'

SYNC_PREFIX_PATH = {
    'ls8_nbar_scene': '/g/data/rs0/scenes/nbar-scenes-tmp/ls8/',
    'ls7_nbar_scene': '/g/data/rs0/scenes/nbar-scenes-tmp/ls7/',
    'ls8_nbart_scene': '/g/data/rs0/scenes/nbar-scenes-tmp/ls8/',
    'ls7_nbart_scene': '/g/data/rs0/scenes/nbar-scenes-tmp/ls7/',
    'ls8_pq_scene': '/g/data/rs0/scenes/pq-scenes-tmp/ls8/',
    'ls7_pq_scene': '/g/data/rs0/scenes/pq-scenes-tmp/ls7/',
    'ls8_pq_legacy_scene': '/g/data/rs0/scenes/pq-legacy-scenes-tmp/ls8/',
    'ls7_pq_legacy_scene': '/g/data/rs0/scenes/pq-legacy-scenes-tmp/ls7/'
}

SYNC_SUFFIX_PATH = {
    'ls8_nbar_scene': '/??/output/nbar/',
    'ls7_nbar_scene': '/??/output/nbar/',
    'ls8_nbart_scene': '/??/output/nbart/',
    'ls7_nbart_scene': '/??/output/nbart/',
    'ls8_pq_scene': '/??/output/pqa/',
    'ls7_pq_scene': '/??/output/pqa/',
    'ls8_pq_legacy_scene': '/??/output/pqa/',
    'ls7_pq_legacy_scene': '/??/output/pqa/'
}

SYNC_COMMAND = """
  {% set work_dir = '/g/data/v10/work/sync/' + params.product + '/' + ds %}
  {% set sync_cache_dir = work_dir + '/cache' %}
  {% set sync_path = params.sync_prefix_path + params.year + params.sync_suffix_path %}
  mkdir -p {{ sync_cache_dir }}
  qsub -N sync_{{ params.product}}_{{ params.year }} \
  -q {{ params.queue }} \
  -W umask=33 \
  -l wd,walltime=20:00:00,mem=3GB -m abe \
  -l storage=gdata/v10+gdata/fk4+gdata/rs0+gdata/if87 \
  -M nci.monitor@dea.ga.gov.au \
  -P {{ params.project }} -o {{ work_dir }} -e {{ work_dir }} \
  -- /bin/bash -l -c \
      "source $HOME/.bashrc; \
      module use /g/data/v10/public/modules/modulefiles/; \
      module load {{ params.module }}; 
      dea-sync -vvv --cache-folder {{sync_cache_dir}} -j 1 --update-locations --index-missing {{ sync_path }}"
"""


def make_sync_task(product):
    submit_sync = SSHOperator(
        task_id=f'submit_sync_{product}',
        ssh_conn_id='lpgs_gadi',
        command=SYNC_COMMAND,
        params={'product': product,
                'sync_prefix_path': SYNC_PREFIX_PATH[product],
                'sync_suffix_path': SYNC_SUFFIX_PATH[product],
                },
        do_xcom_push=True,
    )
    return submit_sync


ingest_products = {
    'ls8_nbar_scene': 'ls8_nbar_albers',
    'ls8_nbart_scene': 'ls8_nbart_albers',
    'ls8_pq_scene': 'ls8_pq_albers'
}

with DAG('schedule_dataset_sync_orchestration',
         default_args=default_args,
         catchup=False,
         schedule_interval=timedelta(days=1),
         template_searchpath='templates/'
         ) as dag:
    for product in synced_products:
        submit_sync = make_sync_task(product)

        # get_qstat_output = SSHOperator(
        #     task_id='get_qstat_output',
        #     command='qstat -xf -F json',
        #     do_xcom_push=True,
        #     dag=dag
        # )

        # TODO Implement an SSH Sensor to wait for the submitted job to be done
        wait_for_pbs = BashOperator(
            task_id=f'wait_for_pbs_sync_{product}',
            bash_command='date',
            dag=dag)

        submit_sync >> wait_for_pbs

        if product in ingest_products:
            ing_product = ingest_products[product]
            # Submit an Ingest Job
            INGEST_COMMAND = """
                module use /g/data/v10/public/modules/modulefiles;
                module load {{ params.module }};
yes Y | dea-submit-ingest qsub --project {{ params.project }} --queue {{ params.queue }} -n 1 -t 15 --allow-product-changes \
  --name ing_{{params.ing_product}}_{{params.year}} {{params.ing_product}} {{params.year}}
            
            """
            # ingest_task = SSHOperator(
            #     task_id=f'submit_ingest_{ing_product}',
            #     ssh_conn_id='lpgs_gadi',
            #     command=INGEST_COMMAND,
            #     params={'ing_product': ing_product},
            #     do_xcom_push=True,
            # )

            # wait_for_pbs >> ingest_task

#####################################
# Ingest
#####################################

INGEST = '''execute_ingest --dea-module ${self:provider.environment.DEA_MODULE}
--queue ${self: provider.environment.QUEUE}
--project ${self: provider.environment.PROJECT}
--stage ${self: custom.Stage}
--year % (year)
--product % (product)'''

ls7_ingest = 'ls7_nbar_albers,ls7_nbart_albers,ls7_pq_albers'
