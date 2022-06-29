from operator import index
from airflow import DAG
import datetime

from sqlalchemy import false, null
from airflow.contrib.hooks.salesforce_hook import SalesforceHook
# from airflow.hooks.S3_hook import S3Hook
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import boto3
import pandas as pd
import json
from sqlalchemy import create_engine


def sfConnection():
    sf=SalesforceHook(conn_id='salesforce_conn')
    query = """SELECT FIELDS(ALL) FROM User limit 200"""
    account = sf.make_query(query).get('records')
    print("first : ",type(account))
    dataframe = pd.DataFrame(account)
    print("second :", type(dataframe))
    csv_data=(dataframe.drop(labels='attributes', axis=1,inplace=False))
    csv_data.to_csv("/home/dict.csv", index=False)

def uploadToS3():
    loginInfo=json.load(open('/login.json'))
    aws_access_key_id = loginInfo['aws_access_key_id']
    aws_secret_access_key = loginInfo['aws_secret_access_key']

    client=boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    client.create_bucket(Bucket='poc2-airflow2')

    with open("/home/dict.csv","rb") as f:
        client.upload_fileobj(f,"poc2-airflow2","user.csv")
        
def downloadFromS3():
    loginInfo=json.load(open('/login.json'))
    aws_access_key_id = loginInfo['aws_access_key_id']
    aws_secret_access_key = loginInfo['aws_secret_access_key']

    client=boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    client.download_file("poc2-airflow2",'user.csv', 'store/user.csv')

def transformationFunc(**kwargs):
    df = pd.read_csv('store/user.csv')

    df.drop_duplicates()

    df['DefaultGroupNotificationFrequency'].replace('N',float('NaN'), inplace=True)

    #column
    df.dropna(how='all', axis=1, inplace=True) 

    #row
    df.dropna(how='all', axis=0, inplace=True)

    def getIndexes(dfObj, value):
        listOfPos = list()
        result = dfObj.isin([value])
        seriesObj = result.any()
        columnNames = list(seriesObj[seriesObj == True].index)
        for col in columnNames:
            rows = list(result[col][result[col] == True].index)
            for row in rows:
                listOfPos.append((row, col))
        return listOfPos

    for column in df:
        for value in df[column]:
            if(str(value)== 'nan'):
                indexOfNaN = getIndexes(df,value)
                for val in indexOfNaN:
                    #print(val[0],val[1], type(val))
                    if(val[0]-1 != -1):
                        #print(df[val[1]][val[0]-1])
                        df[val[1]].replace(df[val[1]][val[0]],df[val[1]][val[0]-1], inplace=True)
                    else:
                        df[val[1]].replace(df[val[1]][val[0]],df[val[1]][len(df[val[1]])-1], inplace=True)
    
    # df.to_csv("/home/s3Download.csv", index=False)
    ti = kwargs['ti']
    ti.xcom_push(key='msg', value=df)

def transToPgs(**kwargs):
    ti = kwargs['ti']
    data =ti.xcom_pull(key='msg')
    # print(data)

    # postgres_conn = PostgresHook(postgres_conn_id='postgres_conn')
    # postgres_conn = postgres_conn.get_conn()

    postgres_str = 'postgresql://airflow:airflow@postgres:5432/airflow'
    postgres_conn = create_engine(postgres_str)

    data.to_sql('final_data', con=postgres_conn, if_exists='replace', index=False)

    # records =postgres_conn.execute("SELECT * FROM final_data").fetchall()


default_args={
    'start_date':datetime.datetime(2022,1,1)
}
    
with DAG("SalesforceToS3",default_args=default_args,schedule_interval="@once",catchup=False) as dags:

    t1 = PythonOperator(
        task_id='salesforce_connection',
        python_callable=sfConnection
    )

    t2 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=uploadToS3
    )

    t3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=downloadFromS3,
    )

    t4 = PythonOperator(
        task_id='transformation',
        python_callable=transformationFunc,
        provide_context = True
    )

    t5 = PythonOperator(
        task_id='transfer_to_postgres',
        python_callable=transToPgs,
        provide_context = True
    )
    
t1 >> t2 >> t3 >> t4 >> t5