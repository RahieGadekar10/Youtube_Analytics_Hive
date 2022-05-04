import airflow
from airflow.models import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

with DAG(dag_id="hive_project" , schedule_interval= "@daily" , start_date=datetime(2020,1,1) , catchup = False) as hive_dag : 
	starttask = DummyOperator(task_id="Start")
	createmaintable = BashOperator(task_id = "Create_Main_Table" , bash_command = "hive -f /home/bigdata/spark_project/create_main_tables.hql")
	createdatatable = BashOperator(task_id = "Create_Data_Table" , bash_command = "hive -f /home/bigdata/spark_project/create_data_tables.hql")
	runpython = BashOperator(task_id = "Import_To_Main_Table" , bash_command = "python /home/bigdata/spark_project/code.py")
	runmain = BashOperator(bash_command="hive -f /home/bigdata/spark_project/bigdata_import.hql" , task_id = "Main_Table_Import")
	rundata = BashOperator(bash_command="hive -f /home/bigdata/spark_project/data_table_import.hql" , task_id = "Data_Table_Import")
	runanalytics = BashOperator(bash_command="hive -f /home/bigdata/spark_project/analytics_data_import.hql" , task_id = "Analytics_Table_Import")
	runsubtable = BashOperator(bash_command="python /home/bigdata/spark_project/subtables_insert_data.py" , task_id = "Sub_Table_Import")
	rundummy = DummyOperator(task_id = "Successful")

starttask>>createmaintable>>createdatatable>>runpython>>runmain>>rundata>>runanalytics>>runsubtable>>rundummy
