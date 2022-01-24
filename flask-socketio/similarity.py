import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import json
import env

DEFAULT_SIMILARITY_VALUE = 0.4
def similarity_word(colum,value,filename):    
    import redis
    redis = redis.Redis(host=env.HOST,port=env.PORT)
        
    redisdata = redis.lrange(filename, -1,-1)
    datastring = redisdata[0]
    datastring = datastring.decode('utf-8')
        
    # data yang digunakan
    listdata = json.loads(datastring)

    df = pd.DataFrame(listdata)
    # insert to first row data
    df = df[[colum]]
    df.loc[-1] = [value]
    df.index = df.index + 1 
    df.sort_index(inplace=True)

    corpus = df[colum]
    # from sklearn.feature_extraction.text import CountVectorizer
    vectorizer = CountVectorizer()
    trsfm=vectorizer.fit_transform(corpus)
    # from sklearn.metrics.pairwise import cosine_similarity
    hasil = pd.DataFrame(cosine_similarity(trsfm[0:1],trsfm))
    hasil_1 = hasil.T
    df['similarity'] = hasil_1
    
    # remove first row
    df = df.iloc [ 1 : , : ]
    
    # hanya memunculkan yang memiliki nilai similarity >= 0.4
    df = df.loc[df['similarity'] >= DEFAULT_SIMILARITY_VALUE]
    print (df)