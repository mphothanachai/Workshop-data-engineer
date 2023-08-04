#Import to use the function of the workshop.
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import os


MYSQL_CONNECTION = "mysql_default"  # The name of the connection set in Airflow.
CONVERSION_RATE_URL = os.getenv("CONVERSION_RATE_URL") # Api link

#ALL path (set variable to store the file path and give a name to the destination file.)
#this path is default of data in google cloud(data lake),difference in the filename that I set.
mysql_output_path = "/home/airflow/gcs/data/audible_data_merged.csv"
conversion_rate_output_path = "/home/airflow/gcs/data/conversion_rate.csv"
final_output_path = "/home/airflow/gcs/data/output.csv"


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



def  get_conversion_rate(conversion_rate_path):
r = requests.get(CONVERSION_RATE_URL)
result_conversion_rate = r.json()
df = pd.DataFrame(result_conversion_rate)

#Change the index, which is currently set as dates, to a new column named 'date,' and then save the DataFrame as a CSV file.
df = df.reset_index().rename(columns={"index": "date"})
df.to_csv(conversion_rate_path, index=False)
print(f"Output to {conversion_rate_path}")

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


with DAG(
"air_flow_DAG", #name of dag
start_date=days_ago(1),#Set the start date of the DAG's to yesterday.
schedule_interval="@daily",#run this dag with daily
tags=["airflow"]
) as dag:
      
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
# Create task 't4' using BashOperator to work with BigQuery for running a bash command.
t4 = BashOperator(
task_id="load_to_bq",
bash_command="bq load --source_format=CSV --autodetect datawarehouse.audible_data gs://asia-southeast1-airflow-bd6f87c8-bucket/data/output.csv"
)

[t1, t2] >> t3 >> t4

