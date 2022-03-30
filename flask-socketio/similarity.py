from calendar import c
from codecs import ignore_errors
from operator import index
from textwrap import indent
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import json
import env
from py_console import console
import redis


DEFAULT_SIMILARITY_VALUE = 0.4
redis = redis.Redis(host=env.HOST, port=env.PORT, decode_responses=True)


def parse_redis_to_list_of_dict(redis_data: list): 
    return json.loads(redis_data[0])


def similarity_word(column_name, value:str, filename:str, similarity_value:float):    
    redisdata = redis.lrange(filename, -1,-1)
    list_of_dict = parse_redis_to_list_of_dict(redisdata)
       
    df = pd.DataFrame(list_of_dict)
    df.loc[-1] = {column_name: value}
    df.index += 1 
    df.sort_index(inplace=True)


    corpus = df[column_name]
    from sklearn.feature_extraction.text import CountVectorizer
    vectorizer = CountVectorizer()
    transform_vector = vectorizer.fit_transform(corpus)
    from sklearn.metrics.pairwise import cosine_similarity
    result_vector_df = pd.DataFrame(cosine_similarity(transform_vector[0:1], transform_vector))
       
    df['similarity'] = result_vector_df.T
    
    # remove first row
    df = df.iloc[ 1:, : ]

    # hanya memunculkan yang memiliki nilai similarity >= 0.4
    df = df.loc[df['similarity'] >= similarity_value]
     
    rows = df['row'].to_list()
    similarity_values = df['similarity'].to_list()

    response = {
        "rows": rows,
        "similarity": similarity_values,
        "length": len(rows)
    }
    
    
    return response
