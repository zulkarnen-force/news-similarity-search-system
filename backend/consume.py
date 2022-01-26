import json
import pandas as pd
import env
import redis

# connect to redis
redis = redis.Redis(host=env.HOST,port=env.PORT)

def insert_to_redis(filename,result_json):
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
    
    # to json
    result = df.to_json(orient="records")
    parsed = json.loads(result)

    print(df)
    insert_to_redis(file,parsed)
    
    # Akui bahwa kami menangani pesan tanpa masalah.
    message.ack()
