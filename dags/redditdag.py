from airflow import DAG
from datetime import datetime
import sys
import os
from airflow.providers.standard.operators.python import PythonOperator
from pipelines.reddit_pipeline import reddit_pipeline

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    'owner': 'Aaroh Jugulum',
    'start_date': datetime(2025, 6, 6)
}

file_postfix = datetime.now().strftime("%Y%m")

dag = DAG(dag_ide = 'etl_reddit_pipeline',
          default_args = default_args,
          catchups = False,
          tags = ['reddit', 'etl', 'pipeline']
)

# now extract the data from reddit
extract = PythonOperator(
    task_id = 'reddit_extract',
    python_callable = reddit_pipeline,
    op_kwargs = {
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'theWeeknd',
        'time_filter': 'day',
        'limit': 100
    }
)