# README #


### 1] What is this repository for? ###

* This repo contains Amazon Marketing Revenue and Fee ETL Data Pipeline for analysis.
* Python - 3.5
* Database - Postgres 10.19

### 2] How do I get set up? ###
* Installing airflow locally: https://airflow.apache.org/docs/apache-airflow/stable/start.html ###
* Version - 1.10.9
* Create a virtual environment with venv and install requirement.txt
* plugins - Plugins are external programs which we can install independently and has a
  specific structure of hooks and operators.

#### airflow needs a home, ~/airflow is the default, but you can lay foundation somewhere else if you prefer (optional) ####
export AIRFLOW_HOME=~/airflow

#### install from pypi using pip ####
pip install apache-airflow

#### initialize the database ####
airflow initdb

#### start the web server in one terminal, default port is 8080 ####
airflow webserver -p 8080
 
#### start the scheduler in the other terminal ####
airflow scheduler

#### visit localhost:8080 in the browser and enable the example dag in the home page ####

#### 3] Initializing Airflow Database:  https://airflow.apache.org/docs/apache-airflow/stable/howto/initialize-database.html ####
airflow db init

#### 4] Creating Connections ####
1. S3:
    Create connetions in your airflow admin for s3 or use your current aws_access_key_id and aws_secret_access_key in the getS3Conn() function for running the s3_to_redshift operatorfor testing locally.
2. Redshift:
    Create a redshift connection(postgres_redshift) in your airflow admin (local tunelling will be needed to access redshift).
3. Google Cloud Platform:
    Create a gcp_connection fot accessing data for search console and GA. (https://cloud.google.com/composer/docs/how-to/managing/connections)
4. Postgres:
	Add you credentials to the postgres_default for connection and table creation
 
 
### 6] For installing new airflow plugins: https://airflow-plugins.readthedocs.io/en/latest/installation.html ###
* https://github.com/airflow-plugins


### 7] Assumptions & Implementation  ###
* Below tables should be present in the database. (Please refer queries.txt)
  1. dim_product
  2. dim_country
  3. dim_date
  4. dim_currency
  5. dim_marketplace
  6. fact_revenue
  7. fact_fee

* Execution date of the airflow DAG is when the DAG gets executed, but we have dataset for 
  the year 2021, either we should get latest updated dataset or need execution date code changes
  or call airflow API with the following parameters.

  sample_input_params = {'conf':{'start_date': '2021-10-01', 'end_date': '2021-11-25'}}
  requests.post("http://localhost:8080/api/experimental/dags/<dag_name>/dag_runs",json=sample_input_params).text

* We only have order lines data, so for product job load I am taking unique products with latest
  updated_date column and applying SCD with appending the modified column and setting is_active
  true.

* Assuming order-lines data as fact_order by not taking order_details out of it as it had
  few columns on order level data.

* We are capturing the CDC by getting latest data by filtering source data on updated_at.

* Data flow and Data modelling images are in dev_images folder

* Please set plugins as your source_folder in pycharm/VS

* Optimal SCD Implementation - https://dwgeek.com/scd-type-2-sql.html/ (To be implemnted)

