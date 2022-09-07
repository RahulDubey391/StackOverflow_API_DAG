from google.cloud import bigquery as bq
import pandas_gbq

class BQOp:
    def __init__(self,configObj):
        self.project = configObj.project
        self.dataset = configObj.dataset
        self.stg_questions_table = configObj.stg_questions_table
        self.stg_answers_table = configObj.stg_answers_table

    def create_dataset(self):
        create_dataset_query = """CREATE SCHEMA IF NOT EXISTS `{i}.{j}`;""".format(i=self.project,j=self.dataset)
        clnt = bq.Client()
        clnt.query(create_dataset_query)
        print('DATASET CREATED')

    def createTable(self,table,cols):
        self.create_dataset()
        schema = ''
        for i in cols:
            schema = schema + i + ' STRING, '
        clnt = bq.Client()
        create_table_query = """CREATE TABLE IF NOT EXISTS `{i}.{j}.{k}` ({l});""".format(
        i=self.project,
        j=self.dataset,
        k=table,
        l=schema)
        print(create_table_query)
        clnt.query(create_table_query)
        print('TABLE CREATED')

    def truncate(self,table):
        clnt = bq.Client()
        query = 'TRUNCATE TABLE %s.%s.%s'%(self.project,self.dataset,table)
        clnt.query(query)

    def BQ_Stage_loader(self,table,data,cols):
        self.createTable(table,cols)
        self.truncate(table)
        table_ref = self.dataset+'.'+table
        pandas_gbq.to_gbq(data,destination_table=table_ref,project_id=self.project,if_exists='append')

    def BQ_Fact_loader(self):
        pass

