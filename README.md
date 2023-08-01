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
 6. Click on the `Cloud Shell` on the right side => Editor to create dag.py for the task execution.![image](https://github.com/mphothanachai/Workshop-data-engineer-/assets/137395742/20987a1e-22a6-4b93-8c5a-c0c27b823e2f)
