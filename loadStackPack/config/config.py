class Config:
    def __init__(self):
        self.max_pages = 20
        self.page_size = 100
        self.write_path = 'gs://stack-api-data'
        self.project = 'business-deck'
        self.dataset = 'STACK_OVERFLOW_SCHEMA'
        self.stg_questions_table = 'STG_QUESTIONS'
        self.stg_answers_table = 'STG_ANSWERS'
        self.fct_questions_table = 'FCT_QUESTIONS'
        self.fct_answers_table = 'FCT_ANSWERS'

configObj = Config()