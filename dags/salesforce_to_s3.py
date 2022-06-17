
from airflow import DAG
import datetime
from airflow.contrib.hooks.salesforce_hook import SalesforceHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
import json
import pandas as pd

default_args={
    'start_date':datetime.datetime(2022,1,1)
}

def abcd():
    sf=SalesforceHook(conn_id='salesforce_conn')
    query = """SELECT FIELDS(ALL) FROM User limit 200"""
    account = sf.make_query(query).get('records')
    print("first : ",type(account))
    dataframe = pd.DataFrame(account)
    print("second :", type(dataframe))
    csv_data=(dataframe.drop(labels='attributes', axis=1,inplace=False))
    csv_data.to_csv("/home/dict.csv", index=False)

def s3_create_bucket():
    hook = S3Hook('s3_conn')
    hook.create_bucket('salesforcetos3bydarshan')


def _upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
    

with DAG("SalesforceToS3",default_args=default_args,schedule_interval="@once",catchup=False) as dags:
    t1=PythonOperator(
        task_id='t1',
        python_callable=abcd
    )

    t2=PythonOperator(
        task_id='t2',
        python_callable=s3_create_bucket
    )


    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=_upload_to_s3,
        op_kwargs={
            'filename': '/home/dict.csv',
            'key': 'dict.csv',
            'bucket_name': 'salesforcetos3bydarshan'
        }
    )

    t1 >> t2 >> upload_to_s3    