import json
import env
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from nltk.tokenize import TweetTokenizer
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from nltk.corpus import stopwords
import re  # impor modul regular expression
import pandas as pd
import string

DEFAULT_SIMILARITY_VALUE = 0.4

def insert_to_redis(filename,result_json):
    import redis
    redis = redis.Redis(host=env.HOST, port=env.PORT)

    redis.rpush(filename, json.dumps(result_json))

def similarity_word_preproses(colum,value,filename):
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
    df['lower'] = df[colum].str.lower()


    def hapus_angka(tweet):
        tweet = re.sub(r"\d+", "", tweet)
        return tweet


    df['h_angka'] = df['lower'].apply(lambda x: hapus_angka(x))

    #import stopword
    stopwords_indonesia = stopwords.words('indonesian')

    #import sastrawi
    factory = StemmerFactory()
    stemmer = factory.create_stemmer()

    # tokenize


    def clean(content):
        content = re.sub(r'-', ' ', content)
        # remove stock market tickers like $GE
        content = re.sub(r'\$\w*', '', content)
        # remove hyperlinks
        content = re.sub(r'https?:\/\/.*[\r\n]*', '', content)
        # only removing the hash # sign from the word
        content = re.sub(r'#', '', content)
        # remove coma
        content = re.sub(r',', '', content)
        # remove angka
        content = re.sub('[0-9]+', '', content)
        # tokenize contents
        tokenizer = TweetTokenizer(preserve_case=False,
                                strip_handles=True, reduce_len=True)
        content_tokens = tokenizer.tokenize(content)

        contents_clean = []
        for word in content_tokens:
            if (word not in stopwords_indonesia and  # remove stopwords
                    word not in string.punctuation):  # remove punctuation
                # contents_clean.append(word)
                stem_word = stemmer.stem(word)  # stemming word
                contents_clean.append(stem_word)
        return contents_clean


    df['token'] = df['h_angka'].apply(lambda x: clean(x))

    # remove punct


    def remove_punct(text):
        text = " ".join([char for char in text if char not in string.punctuation])
        return text


    # simpan di kolom tabel baru tweet
    df['clean'] = df['token'].apply(lambda x: remove_punct(x))

    corpus = df['clean']
    vectorizer = CountVectorizer()
    trsfm = vectorizer.fit_transform(corpus)

    hasil = pd.DataFrame(cosine_similarity(trsfm[0:1], trsfm))

    hasil_1 = hasil.T
    df['similarity'] = hasil_1
    df = df[[colum, 'similarity']]

    # remove first row
    df = df.iloc [ 1 : , : ]
        
    # hanya memunculkan yang memiliki nilai similarity >= DEFAULT_SIMILARITY_VALUE
    df = df.loc[df['similarity'] >= DEFAULT_SIMILARITY_VALUE]
    
    # set to json
    result = df.to_json(orient="records")
    parsed = json.loads(result)
    result_json = json.dumps(parsed, indent=4) 
    
    print(result_json)
    insert_to_redis(filename,parsed)