
# coding: utf-8

import pandas as pd
import pyodbc
import re

'''
read data from SQL SERVER
'''
conn = pyodbc.connect(
    r'DRIVER={SQL Server};'
    r'SERVER=DESKTOP-JJ15EJV;'
    r'DATABASE=KAIJU;'
    r'UID=kaiju;'
    r'PWD=hp5314wc;'
)

cursor = conn.cursor()


df = pd.read_sql("SELECT * FROM KAIJU_TWEET", conn)

df['TweetText'] = df['TweetText'].apply(lambda x: re.sub(' +',' ',x.lower()))
df = df[df['TweetText'].apply(lambda x: len(x.split(' '))>5)]
ids = list(df['TweetID'])


from collections import Counter 
def majority(arr):
    # convert array into dictionary
    freqDict = Counter(arr).most_common()
    if len(freqDict) > 0:
        return freqDict[0][0]
    else:
        return np.NaN


def parse_sentiment(record):
    ID = record['TweetID']
    key_words = record['keywords']
    entity, anger, disgust, fear, joy, sadness, sentiment, sent_score = [], [], [], [], [], [], [], []
    try:
        for kword in key_words:
            entity.append(kword['text'])
            anger.append(kword['emotion']['anger'])
            disgust.append(kword['emotion']['disgust'])
            fear.append(kword['emotion']['fear'])
            joy.append(kword['emotion']['joy'])
            sadness.append(kword['emotion']['sadness'])
            sentiment.append(kword['sentiment']['label'])
            sent_score.append(kword['sentiment']['score'])
    except:
        pass
    return [ID, entity, np.mean(anger), np.mean(disgust), np.mean(fear), np.mean(joy), np.mean(sadness), majority(sentiment), np.mean(sent_score)]        

# read data from mongo db
import numpy as np
import pymongo
import json
Client = pymongo.MongoClient()

db = Client["KAIJU"]
collection_db = db["tweets"]

query_one = {'TweetID':{'$in':ids}}
def read_mongo(collection, query, colnames):
    """ Read from Mongo and Store into DataFrame """

    # Make a query to the specific DB and Collection
    cursor = collection.find(query)
    record_list = []
    
    for record in iter(cursor):
        try:
            record = parse_sentiment(record)
            record_list.append(record)
        except:
            print(record) 
    return pd.DataFrame(record_list,columns=colnames)


colnames = ['TweetID'] + 'entity, anger, disgust, fear, joy, sadness, sentiment, sent_score'.split(', ')
sent_df = read_mongo(collection=collection_db, query=query_one, colnames=colnames)


'''
Merge two dataset and save as final_df.csv
'''

final_df = pd.merge(left=df, right=sent_df, on='TweetID').drop('TweetLang', axis=1)


final_df.to_csv('final_df.csv',index=False,sep='|')


