from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    '0709_dags',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id='t1',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='t2',
        bash_command='date',
    )
    t3 = BashOperator(
        task_id='t3',
        bash_command='date',
    )
    t4 = BashOperator(
        task_id='t4',
        bash_command='date',
    )
    t5 = BashOperator(
        task_id='t5',
        bash_command='date',
    )
    t6 = BashOperator(
        task_id='t6',
        bash_command='date',
    )
    t7 = BashOperator(
        task_id='t7',
        bash_command='date',
    )
    t8 = BashOperator(
        task_id='t8',
        bash_command='date',
    )
    t9 = BashOperator(
        task_id='t9',
        bash_command='date',
    )


    t1 >> [t2, t3] >> t4,
    t5 >> t6 >> [t7, t8, t9]