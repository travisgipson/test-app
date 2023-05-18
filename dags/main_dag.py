import sys
import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# import local  modules
sys.path.append("/app/airflow/scripts")
import lib

############################################
#2. DEFINE AIRFLOW DAG (SETTINGS + SCHEDULE)
############################################

default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'email': ['user@gmail.com'],
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 1
    }

dag = DAG( "YFinance_Daily_Download",
            default_args=default_args,
            description="Collect Stock / ETF Prices For Analysis",
            catchup=False, 
            start_date= dt.datetime(2023, 5, 17), 
            schedule_interval= "* 7 * * *"  
          )  

##########################################
#3. DEFINE AIRFLOW OPERATORS
##########################################

fetch_prices_task = PythonOperator(task_id = "fetch_prices_task", 
                                   python_callable = lib.dag_defs.run_main_task, 
                                   provide_context = True,
                                   dag= dag)     

##########################################
#4. DEFINE OPERATORS HIERARCHY
##########################################

# fetch_prices_task  >> stocks_plot_task >> stocks_table_task
fetch_prices_task
