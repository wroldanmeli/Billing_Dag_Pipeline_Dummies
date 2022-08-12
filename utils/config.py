PROJECT_ID = 'billing-data-migration'
LOCATION = 'US'
QUERIES_FILES = ['start_billing_dummies']

BIGQUERY_TABLE_CONFIG = {
    # projects
    'BILLING_PROJECT_ID'        : 'billing-data-migration',
    # datasets
    'BILLING_PROCESSED_DATASET' : 'billing_wr_test',
    'SP_RUN_DUMMIES'            : 'billing_fury_sp_00_main_dummy',
}
