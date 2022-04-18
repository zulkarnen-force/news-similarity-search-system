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
    
    if (redis.exists(filename) > 0) :
        console.warn('Data telah ada')
        return
    
    try :
        if redis.rpush(filename, json.dumps(data)) != 0 :
            console.success(f'file saved successfully on Redis with filename {filename}', severe=True, showTime=False)
    except DataError as e :
        console.error(e.args, severe=True)
    except Exception as e :
        console.error(e, severe=True)
        
             
        
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
        
        
URL = 'http://localhost:8000/'

def on_message(message):
    
    
    body:dict = json.loads(message.body)
    
    # headers:dict = message.properties['headers']
    # content_type:str = message.content_type
    # filename:str = headers['filename']
    # path:str = headers['path']
    # console.error(path)
    
    console.error(body, severe=True, showTime=False)
    filename:str = body['filename']
    path:str = body['path']
    
    try :
        dataframe = load_file_from_db(URL+path, filename)
    
        json_data = dataframe.to_json(orient='records')
        object_data = json.loads(json_data)
        
        try :
            
            insert_to_redis(filename, object_data)  
            message.ack()

        except Exception as e:
            console.warn(f'{e} ~consume.py', severe=True, showTime=False)
            
        
    except Exception as e:
        console.error(f'{e} ~consume.py-2', severe=True, showTime=False)
        message.ack()       
        
        
    
    
    
    # if dataframe is False:
    #     file = open('backend/data.json', 'r') # testing
    #     insert_to_redis(filename, json.load(file))
    #     message.ack()
        
    # else:
    #     json_data = dataframe.to_json(orient='records')
    #     object_data = json.loads(json_data)
        
    #     insert_to_redis(filename, object_data)
    #     message.ack()
        

    
    


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
