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
