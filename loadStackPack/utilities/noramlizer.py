import pandas as pd
import datetime
import numpy as np

class JSONnormalizer:
    def __init__(self):
        pass

    def normalize(self,json_data):
        df = pd.json_normalize(json_data)
        return df

    def format_date(self,df,col):
        vals = df[col].tolist()
        for i in range(len(vals)):
            if np.isnan(vals[i]):
                pass
            else:
                vals[i] = datetime.datetime.fromtimestamp(vals[i]).strftime('%Y-%m-%d %H:%M:%S')
        df[col] = vals
        return df