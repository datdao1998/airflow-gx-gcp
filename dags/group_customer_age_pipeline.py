import os
from datetime import datetime, timedelta
import calendar

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException


schedule_interval = '0 0 * * *' 
start_date = datetime(2023, 9, 23) 
concurency = 4 
max_active_runs = 1 
catchup = False 
execute_date = "{{  (execution_date-macros.timedelta(days=0)).strftime('%Y-%m-%d')  }}"

default_args = { 
    'owner': 'datdq11', 
    'depends_on_past': False, 
    'retries': 2, 
    'retry_delay': timedelta(minutes=5), 
    'catchup': False, 
    'email_on_retry': False, 
    'execution_timeout': timedelta(minutes=180) 
}

def validate(**context):
    """Validate data using Great Expectations checkpoint."""
    from great_expectations.data_context import DataContext
    
    # Get BigQuery connection details from Airflow
    conn = BaseHook.get_connection("datdq11_bigquerry")
    connection_json = conn.extra_dejson
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = connection_json["key_path"]
    
    # Initialize Great Expectations DataContext
    data_context = DataContext(context_root_dir="/usr/local/airflow/dags/gx")
    
    # Run checkpoint
    result = data_context.run_checkpoint(
        checkpoint_name="checkpoint_customer_v5",
        batch_request=None,
        run_name=None,
    )
    
    # Build data docs
    data_context.build_data_docs()

    print(result)
    
    # Raise exception if validation fails
    if not result["success"]:
        raise AirflowException("Validation of the data is not successful")


with DAG( 
    'group_customer_age_pipeline', 
    default_args=default_args, 
    start_date=start_date, 
    schedule_interval=schedule_interval, 
    concurrency=concurency, 
    max_active_runs=max_active_runs, 
    catchup=catchup 
) as dag:

    start_pipeline = DummyOperator(task_id='start_pipeline') 

    # Great Expectations validation task
    ge_check = PythonOperator(
        task_id="gx_validate_data",
        python_callable=validate,
        provide_context=True,
    )
    
    crawler = BashOperator(
        task_id = "group_customer_age_pipeline",
        bash_command = "cd /usr/local/airflow/dags/dbt_project && dbt run --select customer.group_customer_age",
        dag = dag,
        trigger_rule=TriggerRule.NONE_FAILED
    )

    clean_gx_table = BashOperator(
        task_id="clean_gx_table",
        bash_command="cd /usr/local/airflow/dags/ && python -m utils.clean_temp_table",
        dag=dag,
        trigger_rule=TriggerRule.NONE_FAILED
    )   

    end_pipeline = DummyOperator(task_id='end_pipeline', trigger_rule=TriggerRule.NONE_FAILED) 


    start_pipeline >> ge_check >> crawler >> clean_gx_table >> end_pipeline
