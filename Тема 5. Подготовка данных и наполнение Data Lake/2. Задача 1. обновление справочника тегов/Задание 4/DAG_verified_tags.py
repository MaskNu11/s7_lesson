from airflow.operators.bash import BashOperator
from datetime import datetime, date
from airflow import DAG

import os


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8' 


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}


dag = DAG(
    dag_id='start_verifide_report',
    schedule_interval=None,
    default_args=default_args,
)

task1 = BashOperator(
    task_id='start_verifide_report',
    bash_command="spark-submit --master yarn --deploy-mode cluster verified_tags_candidates.py '2022-05-31' 5 300 '/user/masknu11/data/events' '/user/master/data/snapshots/tags_verified/actual' '/user/masknu11/data/analytics/verified_tags_candidates_d5'",
    retries=3,
    dag=dag,
) 

task1