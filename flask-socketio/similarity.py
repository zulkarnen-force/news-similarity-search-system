import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import json
import env
from py_console import console
import redis

DEFAULT_SIMILARITY_VALUE = 0.4
redis = redis.Redis(host=env.HOST, port=env.PORT, decode_responses=True)


def parse_to_dict(redis_data: list): 
    
    try:
        
        return json.loads(redis_data[0])
    except IndexError as e:
        raise e
    except:
        raise Exception('Error from load Redis')
        

def similarity_word(request):
    console.info('REQUEST: ', request)
    
    filename = request['filename']
    column_name = request['column_name']
    text = request['text']
    similarity_value = float(request['similarity'])
    cell = request['cell']

    if redis.exists(filename) == 0 :
        raise Exception(f'data {filename} not exist in Redis')

    try :
        redisdata = redis.lrange(filename, -1, -1)
        redis_dict = parse_to_dict(redisdata)
    except Exception as err:
        raise err
    
    try:
   
        df = pd.DataFrame(redis_dict)
        
        df.loc[-1] = {column_name: text}
        df.index += 1 
        df.sort_index(inplace=True)

        corpus = df[column_name]
        
        vectorizer = CountVectorizer()
        transform_vector = vectorizer.fit_transform(corpus)
        result_vector_df = pd.DataFrame(cosine_similarity(transform_vector[0:1], transform_vector))
        
        df['similarity'] = result_vector_df.T
        
        # remove first row
        df = df.iloc[ 1:, : ]

        # hanya memunculkan yang memiliki nilai similarity sesuai similarity_value
        df = df.loc[df['similarity'] >= similarity_value]
        df['rows'] = df.index
            
        similarity_values = df['similarity'].to_list()
        rows = df['rows'].to_list()

        response = {
            "result":True if len(similarity_values) != 0 else False ,
            "rows": rows,
            "columnName": column_name,
            "similarity": similarity_values,
            "cell": list(map(lambda row: cell[0]+row, list(map(str, rows)))), 
            "length": len(df['rows']),
        }
        
        console.info('RESPONSE ', response, severe=True, showTime=False)
        return response

    except Exception as err:
        raise err

