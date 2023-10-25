from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define default_args for your DAG
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 10, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create an instance of the DAG
dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='Data pipeline for collecting and analyzing data from Mastodon',
    schedule_interval=timedelta(minutes=5),  # Update this to run every 5 minutes
    catchup=False,
)

# Define tasks in your DAG
# Run Task to get data
run_get_data = BashOperator(
    task_id='run_get_data_task',
    bash_command="python3 /home/mastodon/data_mastodon.py",
    dag=dag,
)

# Task to run the Mapper script
run_mapper = BashOperator(
    task_id='run_mapper_task',
    bash_command="hadoop dfs -cat /home/mastodon/sample.json | python3 /home/mastodon/mapreduce.py | sort -k1,1",
    dag=dag,
)


# Task to run the hbase script
run_insert_hbase = BashOperator(
    task_id='insert_into_hbase',
    bash_command='python3 /home/mastodon/insert_hbase.py',
    dag=dag
)

# Set task dependencies
run_get_data >> run_mapper >> run_insert_hbase

if __name__ == "__main__":
    dag.cli()
