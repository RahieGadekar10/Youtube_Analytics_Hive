# Youtube Analytics
## Data Flow

<img src = "https://github.com/RahieGadekar10/Youtube_Analytics_Hive/blob/ccbf75811f01148695155c0030dc06b2efc6d083/hive_project.png"> </img>

## Requirements : 
- Hadoop
- Hive
- Mysql
- Airflow for scheduling

## Deploying Model 

- Download the github repository using : 
  ```bash
  HTTPS : https://github.com/RahieGadekar10/Youtube_Analytics_Hive.git
  ```
  ```bash 
  SSH : git@github.com:RahieGadekar10/Youtube_Analytics_Hive.git
  ```
  ```bash 
  Github CLI : gh repo clone RahieGadekar10/Youtube_Analytics_Hive
  ```
- To execute the project run the following commands in sequence : 
 ```bash
hive -f create_main_tables.hql 
hive -f create_data_tables.hql 
python code.py
hive -f bigdata_import.hql
hive -f analytics_import.hql 
hive -f data_table_import.hql 
python subtables_insert_data.py 
```

## Real-Time Execution
 ```bash
Start Zookeeper and Kafka Server
```
- Start Producer
 ```bash
spark-submit producer.py 
```
- Start Consumer
 ```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 consumer.py
```
## Scheduled Execution
- DAG is present in HiveDag.py
```bash
start airflow webserver
start airflow scheduler
Goto Airflow webserver
```
- DAG will be present by the name HiveDag.py
- Run/Schedule the DAG to execute prediction operation.
