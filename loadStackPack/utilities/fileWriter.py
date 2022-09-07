import gcsfs
import json 

class FileWriter:
    def __init__(self,format,path,filename):
        self.combine_path = path + '/' + filename + format
     
    def write(self,data):
        fs = gcsfs.GCSFileSystem()
        with fs.open(self.combine_path,'wb') as f:
            json.dump(data,f)

    def dfwrite(self,df):
        df.to_csv(self.combine_path)

