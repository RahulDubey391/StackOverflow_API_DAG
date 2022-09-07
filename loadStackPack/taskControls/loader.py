from stackapi import StackAPI
from datetime import date,timedelta,datetime

class StackDataLoader:
    def __init__(self,configObj):
        self.max_pages = configObj.max_pages
        self.page_size = configObj.page_size

    def get_config(self):
        return {
            'MAX_PAGES':self.max_pages,
            'PAGE_SIZE':self.page_size
        }

    def load_category(self,category):
        print('Loading Done')
        SITE = StackAPI('stackoverflow',max_pages=self.max_pages,page_size=self.page_size)
        res = SITE.fetch(category, fromdate=date.today()-timedelta(days=1), todate=date.today())
        return res 