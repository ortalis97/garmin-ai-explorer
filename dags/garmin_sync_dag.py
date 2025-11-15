"""
Airflow DAG for daily Garmin data synchronization.

This DAG runs daily and syncs new data from Garmin Connect to PostgreSQL.
It automatically detects the last sync date and backfills any missing data.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add src directory to path for imports
import sys
sys.path.insert(0, '/opt/airflow')

from src.tasks import run_daily_sync, run_full_backfill


# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# Daily sync DAG
with DAG(
    dag_id='garmin_daily_sync',
    default_args=default_args,
    description='Sync new Garmin data daily with automatic delta detection',
    schedule_interval='0 6 * * *',  # Run at 6 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['garmin', 'sync', 'daily'],
) as daily_dag:
    
    sync_task = PythonOperator(
        task_id='sync_all_entities',
        python_callable=run_daily_sync,
        op_kwargs={
            'entities': ['activities', 'sleep', 'daily_summary'],
            'activity_lookback_days': 30,
        },
    )


# Manual backfill DAG (triggered manually, not scheduled)
with DAG(
    dag_id='garmin_full_backfill',
    default_args=default_args,
    description='Full historical backfill of Garmin data (manual trigger only)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['garmin', 'backfill', 'manual'],
) as backfill_dag:
    
    backfill_task = PythonOperator(
        task_id='backfill_all_entities',
        python_callable=run_full_backfill,
        op_kwargs={
            # Default: last 3 years. Override via Airflow UI when triggering.
            'entities': ['activities', 'sleep', 'daily_summary'],
        },
    )

