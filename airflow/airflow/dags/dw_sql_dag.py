####################################################################
# DAG Imports
####################################################################
import os
import pyodbc
import pandas as pd
import time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
    
####################################################################
# DAG Callables
####################################################################

def _evaluate_mssql_connector():
    print('PWD : ',os. getcwd())
    start_time = time.time()
    cnx = create_mssql_connection()
    employee_df = pd.read_sql_query("SELECT * FROM salaries", con=cnx)
    employee_df.to_csv('employee.csv',index=False)
    end_time = time.time()
    print("Time Taken PYODBC: ",end_time-start_time)

####################################################################
# DAG Operators
####################################################################

evaluate_python_ODBC = PythonOperator(
    task_id = "evaluate_python_ODBC",
    dag = dag,
    python_callable = _evaluate_mssql_connector,
)

evaluate_BCP = BashOperator(
    task_id = "evaluate_BCP",
    dag = dag,
    bash_command="/Users/vachananand/airflow/dags/performance/scripts/export_data.sh "
)