import csv, os
import datetime
from datetime import datetime
import io
import logging
from os import path
import airflow
from airflow import DAG
from airflow import models
import pathlib

from billing_dag_dummies.utils import (
    PROJECT_ID,
    LOCATION,
    start_logging,
    load_queries
)
location=LOCATION
log_dag = start_logging()
log_dag.warning('DUMMIES Pipeline Starting' )

BASE_DIR = pathlib.Path(__file__).parent.resolve()
QUERIES = load_queries(path.join(BASE_DIR, 'resources', 'queries'))
try:
    from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
    from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
except ImportError:
    log_dag.warning('DUMMIES Invalid Operator Import ' )
    pass
    
with DAG(
    "billing_fury_dummies_pipeline",
    default_args = {
        "owner": "Wilson Roldan",
        "start_date": airflow.utils.dates.days_ago(1),
        "depends_on_past": False,
        "email": ["wilson.roldan@mercadolibre.com.co"],
        "email_on_failure": True,
        "email_on_retry": False,
        "location": location,
        "project_id": PROJECT_ID
    },
    catchup=False,
    description='Trigguer Dummies Stored Procedure  00',
    tags=['dummies', 'pipeline', 'cloud billing']
) as dag: 

    call_sp_dummies = BigQueryInsertJobOperator(
        task_id="call_sp_dummies",
        configuration={
            'query': {
                'query': QUERIES['start_billing_dummies'],
                'useLegacySql': False
            }
        },
        location=location,
    )      

call_sp_dummies
