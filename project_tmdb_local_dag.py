from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable


start_date = datetime(2023, 11, 5)

default_args = {
    'owner': 'cagri',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

accessKeyId = Variable.get("accessKeyId")
secretAccessKey = Variable.get("secretAccessKey")

with DAG('project_tmdb_local', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    t0 = SSHOperator(
        task_id="checking_bronze_buckets",
        command=f"/opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:2.4.0 /dataops/project4_local/project_raw_check.py \
              -aki {accessKeyId} -sak {secretAccessKey}",
        ssh_conn_id='spark_ssh_conn',
        cmd_timeout=None) 

    t1 = SSHOperator(
        task_id="creating_credits_table",
        command=f"/opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:2.4.0 /dataops/project4_local/project_credits.py \
            -aki {accessKeyId} -sak {secretAccessKey}",
        ssh_conn_id='spark_ssh_conn',
        cmd_timeout=None)  

    t2 = SSHOperator(
        task_id="creating_movies_table",
        command=f"/opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:2.4.0 /dataops/project4_local/project_movies.py \
            -aki {accessKeyId} -sak {secretAccessKey}",
        ssh_conn_id='spark_ssh_conn',
        cmd_timeout=None) 

    t3 = SSHOperator(
        task_id="checking_silver_buckets",
        command=f"source /dataops/airflowenv/bin/activate && python /dataops/project4_local/project_clean_check.py \
            -aki {accessKeyId} -sak {secretAccessKey}",
        ssh_conn_id='spark_ssh_conn',
        cmd_timeout=None)
    
    t4 = SSHOperator(
        task_id="writing_to_postgresql",
        command=f"source /dataops/airflowenv/bin/activate && python /dataops/project4_local/project_postgresql.py \
            -aki {accessKeyId} -sak {secretAccessKey}",
        ssh_conn_id='spark_ssh_conn',
        cmd_timeout=None)

    t0 >> t1 >> t2 >> t3 >> t4