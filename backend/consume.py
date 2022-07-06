import json
from click import secho
import pandas as pd
import env
import redis 
from redis.exceptions import DataError
from py_console import console
from amqpstorm import message as msg


BASE_URL = 'http://localhost:8000/'
# connect to redis
redis = redis.Redis(host=env.HOST, port=env.PORT, decode_responses=True)

    
def insert_to_redis(filename, data):
    
    if (redis.rpush(filename, data) != 0) :
        console.success(f'file saved successfully on Redis with filename {filename}', severe=True, showTime=False)
    else:
        raise Exception('Failed save to redis')
    
        
             
        
def load_file_from_db(source:str, filename:str) :
    
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
        object_data = json.loads(json_data)
        
        try :
            
            insert_to_redis(filename, json_data)  
            message.ack()

        except Exception as e:
            console.warn(f'{e} ~consume.py', severe=True, showTime=False)
            
        
    except Exception as e:
        console.error(f'{e} ~consume.py-2', severe=True, showTime=False)
        message.ack()