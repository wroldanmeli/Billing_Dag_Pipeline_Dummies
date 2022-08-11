import csv
import datetime
from datetime import datetime
import io
import logging

import airflow
from airflow import DAG
from airflow import models

from billing_dag_dummies.utils import (
    GCP_PROJECT,
    LOCATION,
    start_logging
)
location=LOCATION
log_dag = start_logging()
log_dag.warning('DUMMIES Pipeline Starting HOYWILSON' )

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
        "email_on_retry": False
    },
    start_date=datetime(2022, 8, 9),
    #schedule_interval='0 1 * * *',
    catchup=False,
    description='Trigguer Dummies Stored Procedure  00',
    tags=['dummies', 'pipeline', 'cloud billing']
) as dag: 

   call_sp_dummies = BigQueryInsertJobOperator(
        task_id="call_sp_dummies",
        configuration={
            "query": {
                "query": "CALL `billing-data-migration.billing_wr_test.billing_fury_sp_00_main_dummy`(); ",
                "useLegacySql": False,
                }
            },
            location=location,
    )

call_sp_dummies
