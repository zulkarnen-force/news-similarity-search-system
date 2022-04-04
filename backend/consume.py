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
    
    if (redis.llen(filename) > 0) :
        console.info(redis.llen(filename))
        console.warn('Data telah ada')
        return
    
    try :
        if redis.rpush(filename, json.dumps(data)) != 0 :
            console.success(f'file saved successfully on Redis with {filename}', severe=True, showTime=False)
    except DataError as e :
        console.error(e.args, severe=True)
    except Exception as e :
        print(e)
        
             
        
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
        
        


def on_message(message):
    
    console.info(message.properties, severe=True, showTime=False)
    
    headers = message.properties['headers']
    
    filename:str = headers['filename']
    source:str = headers['source']

    try :
        dataframe = load_file_from_db(source, filename)
    
        json_data = dataframe.to_json(orient='records')
        object_data = json.loads(json_data)
        
        try :
            
            insert_to_redis(filename, object_data)  
            message.ack()

        except Exception as e:
            console.error(e, severe=True, showTime=False)
            pass
        
    except Exception as e:
        console.error(e, severe=True)
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
