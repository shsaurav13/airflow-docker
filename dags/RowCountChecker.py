from airflow.sdk import DAG,task,BaseOperator
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

class RowCountChecker(BaseOperator):
    
    def __init__(self,name:str,**kargs):
        super().__init__(**kargs)
        self.name=name
    
    def execute(self,context):
        print('Hello from RowCountExecutor',{self.name})


with DAG(dag_id='custom_first_dag',
    schedule='@daily',
    start_date=datetime(2026,10,14),
    catchup=False,
    tags=['mysql']) as dag:

    CountCheck=RowCountChecker(task_id='FirstCustom_DAG',name='FirstCustomDAG')
    
    create_summary = SQLExecuteQueryOperator(
        task_id="create_summary_table",
        conn_id="mysql-connection",
        sql="""
            CREATE TABLE IF NOT EXISTS bank.job_summary AS
            SELECT job, COUNT(*) AS customers, AVG(balance) AS avg_balance
            FROM bank.bankemployee
            GROUP BY job;
        """,
    )



    CountCheck>>create_summary


