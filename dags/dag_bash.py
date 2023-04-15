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
    'start_date': datetime(2023, 1, 1)
}


dag = DAG(
    dag_id='load_ods_layer',
    schedule_interval=None,
    default_args=default_args,
)

task1 = BashOperator(
    task_id='start_bash_command',
    bash_command="spark-submit --master yarn --deploy-mode cluster partition.py 2022-05-31 '/user/master/data/events' '/user/masknu11/data/events'",
    retries=3,
)

task1
