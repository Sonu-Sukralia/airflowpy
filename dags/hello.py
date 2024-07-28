from airflow import DAG
from airflow.operators.bash import DashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'datamasterylab',
    'start_date': datetime(year:2024,month:1,dat:25),
    'catchup': False
    
}

dag = DAG(
    dag_id: 'hello_world',
    default_args= default_args,
    schedule= timedelta(days=1)
)



t1 = DashOperator(
    task_id = 'hello world',
    bash_command= 'echo "hello world"',
    dag= dag

)

t2 = DashOperator(
    task_id = 'hello dml',
    bash_command= 'echo "hello data mastery lab"',
    dag= dag

)


