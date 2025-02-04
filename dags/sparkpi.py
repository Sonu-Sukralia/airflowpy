from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 7, 31),
    'catchup': True
}

dag = DAG(
    'spark_kubernetes_example',
    default_args=default_args,
    description='A DAG to submit Spark jobs on Kubernetes',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['example'],
)

spark_operator = SparkKubernetesOperator(
    task_id='spark_pi',
    namespace='spark-jobs',
    application_file="spark-examples.yml",  # Adjust path as needed
    kubernetes_conn_id='k8s',
    do_xcom_push=False,
    dag=dag,
)

spark_operator
