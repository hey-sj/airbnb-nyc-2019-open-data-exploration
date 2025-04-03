from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import data_cleaning
import data_transformation
import data_analysis

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='A simple data pipeline',
    schedule_interval='@daily',
)

input_path = Variable.get('input_path', default_var='data/ABNB_NYC_2019.csv')
output_path = Variable.get('output_path', default_var='data/cleaned_data.csv')

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=data_cleaning.clean_data,
    op_kwargs={'input_path': input_path, 'output_path': output_path},
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=data_transformation.transform_data,
    op_kwargs={'input_path': output_path},
    dag=dag,
)

analyze_data_task = PythonOperator(
    task_id='analyze_data',
    python_callable=data_analysis.analyze_data,
    dag=dag,
)

clean_data_task >> transform_data_task >> analyze_data_task