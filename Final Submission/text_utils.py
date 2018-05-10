import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
from wordcloud import WordCloud
import spacy
import ast
import re
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import normalize


def text_preprocessing(text):
    text = text.strip()
    text = re.sub('\d+', '', text)
    text = re.sub('\s+', ' ', text)
    text = re.sub(r'(.)\1{2,}',r'\1',text)
    
    return text

def filter_entity(entity_list):
    entity_list = ast.literal_eval(entity_list)
    pacific_rim = ('pacific rim, pacific rim uprising')
    filtered_list = []
    for entity in np.array(entity_list):
        entity = re.sub('\s+', ' ', entity.lower())
        if entity not in pacific_rim:
            if entity in entity_dict.keys():
                entity_dict[entity]+=1
            else:
                entity_dict[entity]=1
            filtered_list.append(entity)
    return np.NaN if len(filtered_list)==0 else filtered_list

def pacific_rim_filtering(x):
    if 'pacific rim' in x and x != 'pacific rim uprising':
        return 'pacific rim'
    if 'rim uprising' in x:
        return 'pacific rim'
    else:
        return x
    
def print_key_word_sentence(word, df):
    for sentence in df['TweetText']:
        if word in sentence:
            print(sentence)
            print('\n')

def custom_colloc(sentence, gram_dict):
    for i in gram_list:
        if i in sentence:
            sentence = re.sub(i, gram_dict[i], sentence)
    return sentence  

def pacific_rim_filtering(x):
    if 'pacific rim' in x or 'rim uprising' in x or 'pacificrim' in x and x != 'pacific rim uprising':
        return 'pacific rim'
    elif 'tomb rider' in x:
        return 'tomb rider'
    elif '353 9179' in x:
        return 'phone number'
    elif 'amp' in x:
        return ''
    else:
        return x
    

def word_cloud_entity(df):
    entity_series = df['entity']
    string = ''
    df.drop_duplicates(subset='TweetText',inplace=True)
    for entity_list in entity_series:
        if entity_list is np.NaN:
            pass
        else:
            for entity in entity_list:
                entity = pacific_rim_filtering(entity)
                if 'pacific rim' not in entity:
                    string = string + entity + ' '
    
    wordc = WordCloud(width=1600, height=800, max_font_size=200, background_color='white', stopwords=['pacific rim','new pacific','movie','movies','film','video','cinema','good','bad']).generate(string)
    plt.figure(figsize=(12,10))
    plt.imshow(wordc, interpolation='bilinear')
    plt.axis('off')
    plt.show()
    return wordc