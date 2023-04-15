from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

def test():
    print(os.getcwd())


default_args = {
            'owner': 'airflow',
            'start_date':datetime(2020, 1, 1),
            }

dag_spark = DAG(
            dag_id = "start_sparkoperator_load_ods_layers",
            default_args=default_args,
            schedule_interval=None,
            )

test_task = PythonOperator(
    task_id='test',
    python_callable=test,
)


# объявляем задачу с помощью SparkSubmitOperator
spark_submit_local = SparkSubmitOperator(
                        task_id='spark_submit_task',
                        dag=dag_spark,
                        application ='/partition.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ['2020-05-01','/user/master/data/events', '/user/masknu11/data/events'],
                        conf={
                    "spark.driver.maxResultSize": "20g"
                    },
                        executor_cores = 2,
                        executor_memory = '2g',
                        )

(
test_task >>
spark_submit_local
)
