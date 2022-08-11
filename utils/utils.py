import logging,  pytz, time, re , os, sys 
from google.cloud import logging as cloudlogging
from logging import handlers
from datetime import datetime, time 
from datetime import date 
from pytz import timezone, utc

logger    = logging.getLogger(__name__)
FORMATTER = logging.Formatter("%(asctime)s — %(funcName)20s() — %(levelname)s — %(message)s")

# Obtencion Hora local para log 
def customTime(*args):
    utc_dt = utc.localize(datetime.utcnow())
    cfa_tz = timezone("America/Bogota")
    converted = utc_dt.astimezone(cfa_tz)
    return converted.timetuple()

# Definiicion de Handler de Log en Cloud Explorer 
def get_handler():
    try:
        log_client  = cloudlogging.Client()
        log_handler = log_client.get_default_handler()
        log_handler.setFormatter(FORMATTER)
        logging.Formatter.converter = customTime
        return log_handler
    except Exception as e: 
        print(str(e)) 

# Logger de escritura       
def get_logger(logger_name :str):
    try:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG) 
        logger.addHandler(get_handler())
        logger.propagate = False
        return logger
    except Exception as e: 
        print(str(e)) 

# Inicia el Log 
def start_logging():
    global cf_logger 
    try:
        cf_logger = get_logger("airflow")
        cf_logger.info("Starting Instance DAG Dummies")
        return cf_logger
    except Exception as e: 
        print(str(e))    
        return 