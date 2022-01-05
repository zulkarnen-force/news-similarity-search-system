# from amqpstorm import Connection
import json
import pandas as pd

def insert_to_redis(filename,result_json):
    import redis
    import json
    from redis.commands.json.path import Path   
    redis = redis.Redis(host= 'localhost',port= '6379')

    redis.json().set(filename, Path.rootPath(), result_json)

    result = redis.json().get(filename)
    print(result)

def on_message(message):
    loadfile = json.loads(message.body)
    link = "http://127.0.0.1:8000"
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

    result = df.to_json(orient="records")
    parsed = json.loads(result)
    result_json = json.dumps(parsed, indent=4)

    insert_to_redis(file,result_json)
    
    # Akui bahwa kami menangani pesan tanpa masalah.
    message.ack()