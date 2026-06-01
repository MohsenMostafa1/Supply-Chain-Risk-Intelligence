from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'orion',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_spark_etl():
    # Spark submit job to read HDFS Parquet and write to PostgreSQL
    cmd = """
    spark-submit --master local[*] \
      --driver-class-path postgresql-42.5.0.jar \
      --jars postgresql-42.5.0.jar \
      ./processing/batch/hdfs_to_postgres.py
    """
    subprocess.run(cmd, shell=True, check=True)

def trigger_kubeflow_pipeline():
    # Trigger Kubeflow pipeline using kfp client
    import kfp
    client = kfp.Client(host='http://kubeflow-pipelines-ui:8080')
    client.run_pipeline(
        experiment_name='orion_daily_retraining',
        pipeline_id='your-pipeline-id',
        run_name=f'training_run_{datetime.now().isoformat()}'
    )

dag = DAG(
    'orion_batch_etl_and_retraining',
    default_args=default_args,
    description='ETL from HDFS to PostgreSQL then retrain model',
    schedule_interval='0 2 * * *',  # daily at 2 AM
    catchup=False,
)

etl_task = PythonOperator(
    task_id='hdfs_to_postgres_etl',
    python_callable=run_spark_etl,
    dag=dag,
)

retrain_task = PythonOperator(
    task_id='trigger_kubeflow_retraining',
    python_callable=trigger_kubeflow_pipeline,
    dag=dag,
)

etl_task >> retrain_task
