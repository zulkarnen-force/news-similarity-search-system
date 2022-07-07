import json
import pandas as pd
from pandas import DataFrame
import env
import redis 
from redis.exceptions import DataError
from py_console import console

BASE_URL = 'http://localhost:8000/'
# connect to redis
redis = redis.Redis(host=env.HOST, port=env.PORT, decode_responses=True)

    
def insert_to_redis(filename, data):
    
    file = redis.rpush(filename, data)
    if (file == 0):
        raise Exception('failed save to redis')
    else:
        console.success('success to save to redis', severe=True)

def load_file_from_db(source:str, filename:str) -> DataFrame :
    
    try :
         
        if filename.lower().endswith(('.xlsx','.xlx','.xls')) :
            return pd.read_excel(source, engine='openpyxl')
        elif filename.lower().endswith('.csv'):
            return pd.read_csv(source)
        elif filename.lower().endswith('.json') :
            return pd.read_json(source)
        else :
            raise Exception('Erorr Format: Wrong file format: {}'.format(filename.lower().split('.')[-1]))
    
    except FileNotFoundError as e :
        raise e
    except Exception as e :
        console.error(e)
        

def on_message(message):
    
    body:dict = json.loads(message.body)
    console.info(body, severe=True, showTime=False)
    filename:str = body['filename']
    path:str = body['path']
    
    try :
        dataframe = load_file_from_db(BASE_URL+path, filename)
        json_data = dataframe.to_json(orient='records')
        insert_to_redis(filename, json_data);
        message.ack()
        
    except Exception as e:
        console.error(f'{e} ~consume.py-2', severe=True, showTime=False)
        message.ack()