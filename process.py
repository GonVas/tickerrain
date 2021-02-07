import math

import asyncio

import redis

import pprint
import time
import functools

from datetime import datetime
from dateutil import tz
import datetime

import news
import spacy

from spacy import displacy

import nltk
import pandas as pd
import re

import spacy
import nltk
nltk.download('vader')
nltk.download('vader_lexicon')

from nltk.sentiment.vader import SentimentIntensityAnalyzer

from spacy import displacy


r = redis.Redis(
host='localhost',
port=6379,)

r = redis.Redis(db=7)


processed_posts = []


def proccess_data():
    all_mets = {}
    all_counts = {}

    cursor = 0

    while(True):
        cursor, posts_ret = r.scan(cursor, "submi:*", 100)
        for post in posts_ret:
            tickers_ment = r.hget(post, "tickers_ment").decode("utf-8") 
            if(tickers_ment != ""):
                score = max(int(r.hget(post, "score")), 1)
                #score = 0.1 + math.log(score+0.1)/math.log(10)
                for tick in tickers_ment.split(';'):
                    if(tick in all_mets):
                        all_mets[tick] = [all_mets[tick][0] + score, all_mets[tick][1] + 1]
                    else:
                        all_mets[tick] = [score, 1]

        if(cursor == 0):
            break

    all_mets.pop('ARE', None)
    all_mets.pop('T', None)
    all_mets.pop('FOR', None)
    all_mets.pop('TWO', None)
    all_mets.pop('BE', None)
    all_mets.pop('X', None)
    all_mets.pop('AA', None)
    all_mets.pop('L', None)
    all_mets.pop('LOVE', None)
    all_mets.pop('OR', None)
    all_mets.pop('RE', None)
    all_mets.pop('FL', None)
    all_mets.pop('REAL', None)
    all_mets.pop('OUT', None)
    all_mets.pop('SAFE', None)
    all_mets.pop('SO', None)
    all_mets.pop('ON', None)
    all_mets.pop('IP', None)
    all_mets.pop('BIG', None)
    all_mets.pop('LAND', None)
    all_mets.pop('GOOD', None)
    all_mets.pop('SI', None)
    all_mets.pop('RIDE', None)
    all_mets.pop('MAR', None)
    all_mets.pop('UK', None)
    all_mets.pop('TV', None)
    all_mets.pop('M', None)
    all_mets.pop('Y', None)
    all_mets.pop('PS', None)
    all_mets.pop('R', None)
    all_mets.pop('EVER', None)
    all_mets.pop('PLUG', None)
    all_mets.pop('AI', None)
    all_mets.pop('WELL', None)


    all_m = {}
    all_counts = {}

    for key, val in all_mets.items():
        all_m[key] = math.log(val[0]+0.01)/math.log(10)
        all_counts[key] = val[1]

    all_mets = all_m

    for key, val in all_mets.items():
        all_mets[key] = math.log(val+0.01)/math.log(10)

    all_mets = {k: v for k, v in sorted(all_mets.items(), key=lambda item: item[1])}
    all_counts = {k: v for k, v in sorted(all_counts.items(), key=lambda item: item[1])}


    return all_mets, all_counts


last_process_idx = 5


def get_last_process():
    global last_process_idx

    last = r.lrange("last10keys", 0, 9)[last_process_idx]
    post = r.hgetall(last)

    last_process_idx = (last_process_idx + 1) % 10

    p_type = 'submission'
    tickers_ment = str(post[b'tickers_ment'].decode("utf-8"))
    process_body = str(post[b'process_body'])

    p_title = None
    p_score = None
    p_body = None

    if(b'comment_body' in post):
        p_type = "comment"
        p_body = str(post[b'comment_body'].decode("utf-8"))
        p_score = str(post[b'comment_score'])
    else:
        p_title = str(post[b'title'])
        p_score = str(post[b'score'])
        p_body = str(post[b'body'].decode("utf-8"))


    return {"type":p_type,
            'title': p_title,
            'body':p_body,
            'process_body':process_body,
            'tickers_ment':tickers_ment,
            'score':p_score,
            }


def create_pandas():
    data_all = []
    cursor = 0
    while(True):
        cursor, posts_ret = r.scan(cursor, "submi:*", 100)
        for post in posts_ret:
            post_content = r.hgetall(post)

            post_id = post
            post_score = post_content[b'score'].decode("utf-8")
            post_body = post_content[b'body'].decode("utf-8")
            post_title = post_content[b'title'].decode("utf-8") + " " + post_body
            post_created = post_content[b'created'].decode("utf-8")

            data_all.append([post_id, post_score, post_body, post_title, post_created])


        if(cursor == 0):
            break

    df = pd.DataFrame(data_all, columns=['id', 'score', 'body', 'title', 'created']) 
    return df





def process_tickers(text):
    tickers_ment = set([word.split("$")[-1] for word in text.split() if word.startswith('$')])

    text = re.sub('[^a-zA-Z0-9 \n\.]', ' ', text)
    text = [' '.join(filter(str.isupper, word.split())) for word in text.split()]
    #tickers_ment = list(set([word for word in text.split(" ")]).intersection(set(tickers)))
    tickers_ment = tickers_ment.union(set([word for word in text]))

    tickers_ment = tickers_ment.intersection(set(news.tickers))

    list_tickers = [ticker.upper() for ticker in tickers_ment] 

    print(';'.join(list_tickers))

    return ';'.join(list_tickers)
    


nlp = spacy.load("en_core_web_lg")

def sentiment(row):
    #sentence = processed_sentence['body'].strip()

    #tickers_ment = [ticker for ticker in processed_sentence['tickers_ment'].split(';')]

    tickers_ment = row['tickers']
    sentence = row['body'].strip()

    tk_spans = []

    doc = nlp(sentence)

    sid = SentimentIntensityAnalyzer()
    sentiment = sid.polarity_scores(doc.text)

    if(tickers_ment != [''] and len(tickers_ment) > 1):
        for ticker in tickers_ment.split(';'):
            pos = doc.text.find(ticker)
            #tk_spans.append(doc.char_span(pos, pos + len(ticker), label="ORG"))
            span = doc.char_span(pos, pos + len(ticker), label="ORG")
            #import pudb; pudb.set_trace()
            print(f"ticker : {ticker} and tickerment {tickers_ment}, span {span}")          
            #try:
            #    #doc.ents = [span if e.text == ticker else e for e in doc.ents]
            #except Exception as e:
            #    print(e)
            new_ents = []
            for idx, ent in enumerate(doc.ents):
                new_ents += [span] if ent.text == ticker else [ent]

            if(ticker == "EBAY"):
                import pudb; pudb.set_trace()

            doc.ents = new_ents

    #doc.ents = list(doc.ents) + tk_spans

    print('Processed another one')

    return doc.ents, sentiment


df = create_pandas()
df['tickers'] = df.apply(lambda row: process_tickers(row.body), axis=1)
ents, sentiments = df.apply(lambda row: sentiment(row), axis=1)

import pudb; pudb.set_trace()



