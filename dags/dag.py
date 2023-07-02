import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2020, 1, 1)
}

dag_spark = DAG(
    dag_id = "datalake_project_etl",
    default_args=default_args,
    schedule_interval=None,
)

calc_geo_analytics = SparkSubmitOperator(
    task_id='geo_analytics',
    dag=dag_spark,
    application ='/home/romaticfi/calc_geo_analytics.py' ,
    conn_id= 'yarn_spark',
    application_args = ["2022-05-31", "/user/master/data/geo/events", "/user/romatincfi/data/analytics","/user/romanticfi/data/geo"],
    conf={
    "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

calc_geo_analytics