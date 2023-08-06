
# Workshop-data-engineer

Practicing a workshop based on what I've learned from my classes

## 1.Introduction
I have data on the number of book purchases, book types, and book prices in each country. I want to use it to analyze opportunities and generate business profits.
##  2.Objective
For create opportunities and business profits.
## 3. Design
We will be using Airflow to orchestrate the following tasks:

1. we have book sales data stored in the following mysql (db).
2. Prepare an API for converting currency from USD to THB.
3. Extract data from the database, manage the data.
4. Extract data from the API and manage the data.
5. Store the data in cloud storage (data lake).
6. Merge the data to combine information and clean it.
7. Store the data in cloud storage (data lake).
8. Send the data to Google BigQuery (data warehouse) for data analysts and data scientists to leverage the data for business insights.
9. Automate data processes using Airflow. 
![Capture](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/6f26e6e4-61ff-4b15-ad1e-ebcf956278c1)

## 3. Google composer
We will use Google Composer with Airflow to create a DAG for writing and managing tasks in the Airflow system using Python code.
 1. Go to the [Google Cloud website](https://cloud.google.com/gcp?utm_source=google&utm_medium=cpc&utm_campaign=na-CA-all-en-dr-bkws-all-all-trial-e-dr-1605212&utm_content=text-ad-none-any-DEV_c-CRE_665735450633-ADGP_Hybrid%20%7C%20BKWS%20-%20EXA%20%7C%20Txt_Google%20Cloud-KWID_43700077224548586-kwd-6458750523&utm_term=KW_google%20cloud-ST_google%20cloud&gclid=Cj0KCQjw2qKmBhCfARIsAFy8buJTDdaAzub_a5_LvTWYEFgQAdcgtYCSYz1NRtQip1_QFm1UJRn_dnMaAiITEALw_wcB&gclsrc=aw.ds&hl=th).
 2. Find Google Composer in Google Cloud to create a cluster.
 ![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/4ccbd050-abd5-48d6-a1bd-396b2c8c0bf8)
 
 3. Wait for 15-30 minutes to create the `environment`. ( When you create a Composer environment, you will also get Google Cloud (data lake) included, so you don't need to create an extra one. )
 4. Go into Airflow and search for  `PyPI packages` to install for all tasks.
	 In this case installs`pymysql` ,`requests` ,`pandas` 

	![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/12f319f9-492d-4ce1-93a6-c0fd6c3b2e58)

 5. Click on the`Airflow webserver` , then click on `Admin`, and finally select `Connections`.
 ![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/e62c72e0-5ee6-48cb-8f5d-da9fe683aff0)
 
 6. Find mysql_default and set connection (credential)
 ![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/54096dba-6034-4a34-b8a8-b44b970bc226)
 7. Click on the `Cloud Shell` on the right side => Editor to create dag.py for the task execution.![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/20987a1e-22a6-4b93-8c5a-c0c27b823e2f) 
 Create python file![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/5986b852-606d-4337-905e-009b04b0545b)
 

 8. Begin the Python file by `import` modules that used in this task.
 ```
#Import to use the function of the workshop.

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import os
```
 9. Set the `variables` used in this workshop
  ```
MYSQL_CONNECTION = "mysql_default"  # The name of the connection set in Airflow.
CONVERSION_RATE_URL = os.getenv("CONVERSION_RATE_URL") # Api link

#ALL path (set variable to store the file path and give a name to the destination file.)
#this path is default of data in google cloud(data lake),difference in the filename that I set.
mysql_output_path = "/home/airflow/gcs/data/audible_data_merged.csv"
conversion_rate_output_path = "/home/airflow/gcs/data/conversion_rate.csv"
final_output_path = "/home/airflow/gcs/data/output.csv"
```
 10. Begin by building the first function to query data from two `MySQL` tables and convert them into DataFrames. Next, use `pandas merge` a left merge using the merge function to combine the entire data from the left DataFrame with the matching data from the right DataFrame. Lastly, `convert` the merged data into a CSV file
  ```
def  get_data_from_mysql(transaction_path):
#Use MySqlHook to connect to MySQL using the connection set up in Airflow.
mysqlserver = MySqlHook(MYSQL_CONNECTION)

#Query data from the database using MySqlHook, and the output will be a pandas DataFrame.
audible_data = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_data")
audible_transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_transaction")

#Merge data From 2 DataFrame
df = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

#Save File CSV to transaction_path ("/home/airflow/gcs/data/audible_data_merged.csv")
#It will be automatically sent to Google Cloud Storage (GCS)
df.to_csv(transaction_path, index=False)
print(f"Output to {transaction_path}")
```
 11. Create the second function named `get_conversion_rate` . This function will pull data from a REST API using the `requests` library, convert it to a `JSON format`, and then transform it into a pandas `DataFrame` for easy data cleaning. Next, change the index, which is currently based on dates, to a new column named 'date,' and finally save the DataFrame as a CSV file.
  ```
def  get_conversion_rate(conversion_rate_path):
r = requests.get(CONVERSION_RATE_URL)
result_conversion_rate = r.json()
df = pd.DataFrame(result_conversion_rate)

#Change the index, which is currently set as dates, to a new column named 'date,' and then save the DataFrame as a CSV file.
df = df.reset_index().rename(columns={"index": "date"})
df.to_csv(conversion_rate_path, index=False)
print(f"Output to {conversion_rate_path}")
```
 12. Create the final function that performs pandas merge on the data and then cleans the data. I will explain the steps by step because it too many commands to explain in this.
  ```
def  merge_data(transaction_path, conversion_rate_path, output_path):

#Read from the file and observe that it uses the path received as a parameter.
transaction = pd.read_csv(transaction_path)
conversion_rate = pd.read_csv(conversion_rate_path)

transaction['date'] = transaction['timestamp']
transaction['date'] = pd.to_datetime(transaction['date']).dt.date
conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

  

#merge 2 DataFrame
final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")

#Convert the price by removing the '$' symbol and converting it to a float.
final_df["Price"] = final_df.apply(lambda  x: x["Price"].replace("$",""), axis=1)
final_df["Price"] = final_df["Price"].astype(float)

 
final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
final_df = final_df.drop(["date", "book_id"], axis=1)

#save file CSV
final_df.to_csv(output_path, index=False)
print(f"Output to {output_path}")
```
Start by using pandas `read_csv` to read the file from the provided path
 ```

#Read from the file and observe that it uses the path received as a parameter.
transaction = pd.read_csv(transaction_path)
conversion_rate = pd.read_csv(conversion_rate_path)

```
In this section, copy the  `timestamp` column by creating a new column named `date` from the `transaction` data. Then, convert the 'data' column to date type and also convert the 'conversion_rate' column to `date type`
```

transaction['date'] = transaction['timestamp']
transaction['date'] = pd.to_datetime(transaction['date']).dt.date
conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

```
The transaction DataFrame is `merged`with the conversion_rate DataFrame using a left merge. Within the 'Price' column, there are dollar signs ('$') represented as strings with `lambda`function. To use the data effectively, these symbols need to be removed, and the column should be converted to a float data type and then `change type` the string to floats.
```

# merge 2 DataFrame
final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")

# Convert the price by removing the '$' symbol and converting it to a float.
final_df["Price"] = final_df.apply(lambda  x: x["Price"].replace("$",""), axis=1)
final_df["Price"] = final_df["Price"].astype(float)
The transaction DataFrame is `merged`with the conversion_rate DataFrame using a left merge. Within the 'Price' column, there are dollar signs ('$') represented as strings with `lambda`function. To use the data effectively, these symbols need to be removed, and the column should be converted to a float data type and then `change type` the string to floats.
```


Multiply the 'price' column by the 'conversion_rate' column. Then, create a new column named 'THBPrice' and `drop `drop the 'date' and 'book_id' columns as they are not used. Finally, convert the DataFrame to a `CSV`file.
```
final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
final_df = final_df.drop(["date", "book_id"], axis=1)


# save file CSV
final_df.to_csv(output_path, index=False)
print(f"Output to {output_path}")
```

 13. Create Default Arguments to define the DAG's workflow as follows
```
with DAG(
"air_flow_DAG", #name of dag
start_date=days_ago(1),#Set the start date of the DAG's to yesterday.
schedule_interval="@daily",#run this dag with daily
tags=["airflow"]
) as dag:
```
 14. Create tasks to use functions and assign tasks.
```
  
#Use PythonOperator for use Python code and call the function (get_data_from_mysql). Apply kwargs (Keyword Argument) to provide the value for the mysql_output_path variable
t1 = PythonOperator(
task_id="get_data_from_mysql",
python_callable=get_data_from_mysql,
op_kwargs={"transaction_path": mysql_output_path},
)
#Use PythonOperator for use Python code and call the function (get_conversion_rate). Apply kwargs (Keyword Argument) to provide the value for the conversion_rate_output_path variable
t2 = PythonOperator(
task_id="get_conversion_rate",
python_callable=get_conversion_rate,
op_kwargs={"conversion_rate_path": conversion_rate_output_path},
)

t3 = PythonOperator(
task_id="merge_data",
python_callable=merge_data,
op_kwargs={"transaction_path": mysql_output_path,
"conversion_rate_path": conversion_rate_output_path,
"output_path": final_output_path},
)
```
 15. Prepare data warehouse with create data warehouse for push data to bigquery and create data set
![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/0f46362a-38eb-41a6-abc6-c676b27d5312)
 16. Create dataset should use same region with airflow If the data doesn't match region, data can't push.
 ![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/a1a5c353-1115-4894-82a4-31266c941c35)
 17. Create data table from dataset in this case table name audible_data
 ![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/7d603aa8-886f-4080-bed0-8d9a68d669d5)
 18. In this task, a bash command is created to execute the following command `bq command` for push to bigquery you can learn from this [BQ  command](https://cloud.google.com/bigquery/docs/bq-command-line-tool) to run the bash command, you will use a BigQuery command to push a CSV file into storage (with autodetect for automatic schema detection). Specify the path, which always begins with 'gs://' for Google Cloud bucket.
```
# Create task 't4' using BashOperator to work with BigQuery for running a bash command.
t4 = BashOperator(
task_id="load_to_bq",
bash_command="bq load --source_format=CSV --autodetect datawarehouse.audible_data gs://asia-southeast1-airflow-bd6f87c8-bucket/data/output.csv"
)
```
19. Setting up Dependencies for Determine the order in which tasks should be executed.
```
[t1, t2] >> t3 >> t4
```
20. Move the DAG file to Cloud Shell, then navigate to the Airflow DAG directory With the command `gsutil cp (location dag) (destination path)` in cloudshell
![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/ccf51b4d-a0b8-4f25-a7aa-5e31224f4763)
21. Let run this dag
![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/5480eed3-ed7a-4b82-91fe-38bdb9b929b3)
22. The run was successful
![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/01e92b61-50e8-4d4d-a806-7afd6950108f)
## Docker(Airflow)
 1. First of all, download [Docker](https://www.docker.com/products/docker-desktop/).
 2. Go to Visual Studio Code and open bash terminal 
![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/0abb405b-5e82-458b-be2e-26b2bac7425c)
3. To deploy Airflow on Docker Compose, you should fetch [docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/2.6.3/docker-compose.yaml) in bash terminal.
```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.3/docker-compose.yaml'
```
4. Make four folders to manage Airflow data
-   `./dags`  - you can put your DAG files here.
    
-   `./logs`  - contains logs from task execution and scheduler.
    
-   `./config`  - you can add custom log parser or add  `airflow_local_settings.py`  to configure cluster policy.
    
-   `./plugins`  - you can put your  [custom plugins](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html)  here.

use this command to make folders
```
mkdir  -p  ./dags  ./logs  ./plugins  ./config
```
5. Create a .env file to hold credentials and this generating a UID.
```
echo  -e  "AIRFLOW_UID=$(id  -u)\nAIRFLOW_GID=0"  >  .env
```
6. Open docker and run this command for prepare database.
```
docker  compose  up  airflow-init
```
7. Now you can start all services.
```
docker compose up
```
.
