from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import date,timedelta,datetime
import pendulum
from loadStackPack.config.config import configObj
from loadStackPack.taskControls.loader import StackDataLoader
from loadStackPack.utilities.fileWriter import FileWriter
from loadStackPack.utilities.noramlizer import JSONnormalizer
from loadStackPack.utilities.bqOperator import BQOp
from loadStackPack.config.sanitizationQueries import FACT_ANSWERS_QUERY,FACT_QUESTIONS_QUERY,SNAP_FACT_ANSWERS,SNAP_FACT_QUESTIONS
import pandas as pd
from google.cloud import bigquery as bq

local_tz = pendulum.timezone("US/Eastern")

dag_args = {
    'owner':'self',
    'email': ['rahuldubey391@gmail.com'],
    'start_date': datetime(2022,9,3,tzinfo=local_tz),
    'email_on_failure':True,
    'email_on_retry':False,
    'retries':0
}

dag = DAG('STACK_DATA_LOAD', 
          description = 'This DAG will load all the data from Stack Overflow API', 
          #schedule_interval='0 10 * * *', 
          max_active_runs=1, 
          catchup=False,
          default_args=dag_args)

def init_DataLoad(**context):
    print('Commencing data pull from StackOverflow API')

def init_BQLoad(**context):
    print('Executing BigQuery Load job for normalized file')

def questionsLoadRun(**context):
    file_name = 'questions_'+str(date.today())
    sq = StackDataLoader(configObj)
    data = sq.load_category('questions')
    norm = JSONnormalizer()
    df = norm.normalize(data['items'])

    df = norm.format_date(df,'last_activity_date')
    df = norm.format_date(df,'creation_date')
    df = norm.format_date(df,'last_edit_date')
    df = norm.format_date(df,'closed_date')
    print(df)
    fw = FileWriter('.csv',configObj.write_path+'/QUESTIONS',file_name)
    fw.dfwrite(df)


def answersLoadRun(**context):
    file_name = 'answers_'+str(date.today())
    sa = StackDataLoader(configObj)
    data = sa.load_category('answers')
    norm = JSONnormalizer()
    df = norm.normalize(data['items'])

    df = norm.format_date(df,'last_activity_date')
    df = norm.format_date(df,'last_edit_date')
    df = norm.format_date(df,'creation_date')
    df = norm.format_date(df,'community_owned_date')
    print(df)
    fw = FileWriter('.csv',configObj.write_path+'/ANSWERS',file_name)
    fw.dfwrite(df)

def BQ_Stage_LoadRun(**context):
    answers_df = pd.read_csv(configObj.write_path+'/'+'ANSWERS'+'/'+'answers_%s'%str(date.today())+'.csv')
    questions_df = pd.read_csv(configObj.write_path+'/'+'QUESTIONS'+'/'+'questions_%s'%str(date.today())+'.csv')

    answers_df.columns = [i.replace('.','_')for i in answers_df.columns]
    questions_df.columns = [i.replace('.','_') for i in questions_df.columns]
    answers_df = answers_df.drop('Unnamed: 0', axis=1)
    questions_df = questions_df.drop('Unnamed: 0',axis=1)
    answers_cols = answers_df.columns
    questions_cols = questions_df.columns
    print(answers_cols)
    print(questions_cols)

    answers_df = answers_df.astype(str)
    questions_df = questions_df.astype(str)
    gbq = BQOp(configObj)
    gbq.BQ_Stage_loader(configObj.stg_questions_table,questions_df,questions_cols)
    gbq.BQ_Stage_loader(configObj.stg_answers_table,answers_df,answers_cols)

def BQ_Fact_LoadRun(**context):
    clnt = bq.Client()

    print('Loading Answers Fact Table')
    clnt.query(FACT_ANSWERS_QUERY)

    print('Loading Questions Fact Table')
    clnt.query(FACT_QUESTIONS_QUERY)

def BQ_Snapshot_LoadRun(**context):
    clnt = bq.Client()
    
    print('Loading Snapshot History load for Answers')
    clnt.query(SNAP_FACT_ANSWERS)

    print('Loading Snapshot History load for Questions')
    clnt.query(SNAP_FACT_QUESTIONS)

with dag:

    A = PythonOperator(
        task_id='DATA_LOAD_INITIATION',
        python_callable=init_DataLoad,
        dag=dag
    ) 

    T1 = PythonOperator(
        task_id='LOAD_QUESTIONS',
        python_callable=questionsLoadRun,
        dag=dag
    )

    T2 = PythonOperator(
        task_id='LOAD_ANSWERS',
        python_callable=answersLoadRun,
        dag=dag
    )

    B = PythonOperator(
        task_id='TRANSFORMATION_INITIATION',
        python_callable=init_BQLoad,
        dag=dag
    )

    T3 = PythonOperator(
        task_id='BIGQUERY_INCREMENTAL_STAGE_LOAD_TASK',
        python_callable=BQ_Stage_LoadRun,
        dag=dag
    )

    T4 = PythonOperator(
        task_id='BIGQUERY_INCREMENTAL_FACT_LOAD_TASK',
        python_callable=BQ_Fact_LoadRun,
        dag=dag
    )

    T5 = PythonOperator(
        task_id='BIGQUERY_SNAPSHOT_FACT_LOAD_TASK',
        python_callable=BQ_Snapshot_LoadRun,
        dag=dag
    )

    A >> [T1,T2] >> B >> T3 >> T4 >> T5
