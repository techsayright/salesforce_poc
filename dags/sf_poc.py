from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from airflow.operators.postgres_operator import PostgresOperator
from simple_salesforce import Salesforce, SalesforceLogin, SFType
import pandas as pd
import boto3
import json

default_args = {
    'owner': 'Darsh',
    "retries": 1,
    'start_date': datetime(2022, 6, 15),
    "retry_delay": timedelta(minutes=5),
}

dag = DAG('sf_poc', default_args=default_args, schedule_interval='@daily')

def getDataFrmSf():
    print("hellow")
    
    loginInfo=json.load(open('/home/login.json'))
    username=loginInfo['username']
    password=loginInfo['password']
    security_token=loginInfo['security_token']
    domain = 'login'


    session_id,instance=SalesforceLogin(username=username,password=password,security_token=security_token,domain=domain)
    sf=Salesforce(instance=instance,session_id=session_id)

    query_soql="""SELECT FIELDS(ALL) FROM User LIMIT 7"""

    recordsAccount=sf.query(query_soql).get('records')
    print("account_query_results: {}".format(len(recordsAccount)))

    df_records=pd.DataFrame(recordsAccount)

    csv=(df_records.drop(labels='attributes',axis=1,inplace=False))

    csv.to_csv('/usr/local/airflow/store_file_airflow/user.csv',index=False) 

def transfer_to_S3():
    # print("hello")
    client=boto3.client('s3',aws_access_key_id="", aws_secret_access_key="")

    client.create_bucket(Bucket='poc2-airflow')

    with open("/usr/local/airflow/store_file_airflow/user.csv","rb") as f:
        client.upload_fileobj(f,"poc2-airflow","user.csv")


t1 = PythonOperator(task_id = "get_data_frm_sf", python_callable= getDataFrmSf, dag= dag)

t2 = PythonOperator(task_id="load_to_s3", python_callable=transfer_to_S3 , dag=dag)

t1 >> t2