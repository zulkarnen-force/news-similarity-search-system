import json
import pandas as pd
import numpy as np
import env
 
def insert_to_redis(filename,result_json):
    import redis
    redis = redis.Redis(host=env.HOST,port=env.PORT)

    redis.rpush(filename, json.dumps(result_json))

def on_message(message):
    loadfile = json.loads(message.body)
    link = env.LINK
    path = "/storage/excel-data/"
    file = str(loadfile["filename"])
    url = link+path+file
    
    def load_data():
        if file.lower().endswith(('.xlsx','.xlx','.xls')) :
            data = pd.read_excel(url)
            return data
        elif file.lower().endswith('.csv'):
            data = pd.read_csv(url)
            return data
        
    file_df = load_data()
    df = pd.DataFrame(file_df)
    df.insert(loc=21, column='row', value=np.arange(len(df)))
    result = df.to_json(orient="records")
    parsed = json.loads(result)
    result_json = json.dumps(parsed, indent=4)  

    print(result_json)
    insert_to_redis(file,parsed)
    
    # Akui bahwa kami menangani pesan tanpa masalah.
    message.ack()
