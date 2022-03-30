from dataclasses import dataclass
from email import header
import json
import pandas as pd
import env
import redis 
from py_console import console


BASE_URL = 'http://localhost:8000/'
# connect to redis
redis = redis.Redis(host=env.HOST, port=env.PORT, decode_responses=True)


def insert_to_redis(filename, data):
    if (redis.rpush(filename, json.dumps(data)) != 0) :
        console.info('Berhasil Simpan ke Redis')
    else: 
        console.error('Failure save to Redis')


def load_file_from_db(url:str, filename:str) :
    
    if filename.lower().endswith(('.xlsx','.xlx','.xls')) :
        data = pd.read_excel(url, engine='openpyxl')
        return data
    elif filename.lower().endswith('.csv'):
        data = pd.read_csv(url)
        return data
    return False
        

def on_message(message):
    
    headers = message.properties['headers']
    filename:str = headers['filename']
    source:str = headers['source']

    console.info(filename, severe=True, showTime=False)

    dataframe = load_file_from_db(source, filename)
    
    if dataframe is False:
        file = open('backend/data.json', 'r')
        insert_to_redis(filename.split('.')[0], json.load(file))
        message.ack()
        
    else:
        json_data = dataframe.to_json(orient='records')
        object_data = json.loads(json_data)
        
        insert_to_redis(filename.split('.')[0], object_data)
        message.ack()
        

    
    


# def on_message(message):
#     loadfile = json.loads(message.body)
#     link = env.LINK
#     path = "/excel-data/"
#     file = str(loadfile["filename"])
#     print(loadfile)
#     url = link+path+file
#     print(url)
    
#     def load_data():
#         if file.lower().endswith(('.xlsx','.xlx','.xls')) :
#             data = pd.read_excel(url, engine='openpyxl')
#             return data
#         elif file.lower().endswith('.csv'):
#             data = pd.read_csv(url)
#             return data
#         return False
        
#     file_df = load_data()
#     df = pd.DataFrame(file_df)
    
#     # to json
#     result = df.to_json(orient="records")
#     parsed = json.loads(result)

#     print(df)
#     insert_to_redis(file, parsed)
    
#     # Akui bahwa kami menangani pesan tanpa masalah.
#     message.ack()
