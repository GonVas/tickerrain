import math
import re
import asyncio
from functools import lru_cache, partial
import time
from datetime import datetime
from dateutil import tz
import datetime
from multiprocessing import  Pool

import spacy
from spacy import displacy
import pandas as pd
import redis
import numpy as np
import dill
import nltk
nltk.download('vader')
nltk.download('vader_lexicon')

from nltk.sentiment.vader import SentimentIntensityAnalyzer

nlp = spacy.load("en_core_web_lg")

@lru_cache(maxsize=32)
def get_ticker_list():
    tickers_pd = pd.read_csv('tickers.csv')

    print('Len of ticker before: ' + str(len(tickers_pd)))

    for index, row in tickers_pd.iterrows():
        if(row['Market Cap'] < 300_000_000):
            tickers_pd = tickers_pd.drop(index)

    print('Len of ticker after: ' + str(len(tickers_pd)))
    tickers = list(tickers_pd['Symbol'])

    # Fix some missing tickers and tickers that can be confused with common words
    tickers.append('SPY')
    tickers.remove('A')
    tickers.remove('T')
    tickers.remove('ARE')
    tickers.remove('FOR')
    tickers.remove('NOW')
    tickers.remove('EDIT')
    tickers.remove('POST')
    tickers.remove('U')
    tickers.remove('D')
    tickers.remove('DD')
    tickers.remove('CAN')
    tickers.remove('C')
    tickers.remove('E')
    tickers.remove('IT')
    tickers.remove('BEST')
    tickers.remove('GO')
    tickers.remove('ALL')
    tickers.remove('CEO')
    tickers.remove('EXPR')
    tickers.remove('RH')
    tickers.remove('EV')
    tickers.remove('PSA')
    tickers.remove('FREE')
    tickers.remove('SEE')
    tickers.remove('TWO')
    tickers.remove('CC')
    tickers.remove('DTE')
    tickers.remove('BE')
    tickers.remove('X')
    tickers.remove('AA')
    tickers.remove('L')
    tickers.remove('LOVE')
    tickers.remove('OR')
    tickers.remove('RE')
    tickers.remove('FL')
    tickers.remove('REAL')
    tickers.remove('OUT')
    tickers.remove('SAFE')
    tickers.remove('SO')
    tickers.remove('ON')
    tickers.remove('IP')
    tickers.remove('BIG')
    tickers.remove('LAND')
    tickers.remove('GOOD')
    tickers.remove('SI')
    tickers.remove('RIDE')
    tickers.remove('MAR')
    tickers.remove('UK')
    tickers.remove('TV')
    tickers.remove('M')
    tickers.remove('Y')
    tickers.remove('PS')
    tickers.remove('R')
    tickers.remove('EVER')
    tickers.remove('PLUG')
    tickers.remove('AI')
    tickers.remove('WELL')

    return tickers


def process_tickers(text):
    tickers_ment = set([word.split("$")[-1] for word in text.split() if word.startswith('$')])

    text = re.sub('[^a-zA-Z0-9 \n\.]', ' ', text)
    text = [' '.join(filter(str.isupper, word.split())) for word in text.split()]
    tickers_ment = tickers_ment.union(set([word for word in text]))

    tickers_ment = tickers_ment.intersection(set(get_ticker_list()))

    list_tickers = [ticker.upper() for ticker in tickers_ment] 

    return list_tickers


def nlp_processing(row, ret_doc=False):
    tickers_ment = row['tickers']

    if(len(tickers_ment) == 0):
        return None, None

    sentence = row['body'].strip()

    tk_spans = []

    doc = nlp(sentence)

    sid = SentimentIntensityAnalyzer()
    sentiment = sid.polarity_scores(doc.text)

    if(len(tickers_ment) > 1):
        for ticker in tickers_ment:
            pos = doc.text.find(ticker)
            span = doc.char_span(pos, pos + len(ticker), label="ORG")    
            try:
                doc.ents = [span if e.text == ticker else e for e in doc.ents]
            except Exception as e:
                print(e)

    return doc, sentiment


def filter_hour(row, day=2):
    import pudb; pudb.set_trace()


def timed_processing(time_mins=360):

    df = pd.read_csv('all_data.csv')  

    df['tickers'] = df.apply(lambda row: process_tickers(row.body), axis=1)
    df["ents"], df["sentiment"] = zip(*df.apply(nlp_processing, axis=1))
    df['created'] = df['created'].apply(lambda created: pd.Timestamp(created, unit='s')) 

    data_df = df.filter(['tickers', 'score', 'sentiment', 'created'])
    data_df['score'] = data_df.score.astype(np.int32)

    ex_tickers = data_df.explode('tickers').dropna()
    ex_tickers['sentiment'] = ex_tickers['sentiment'].apply(lambda row: row['compound'])
    ex_tickers['count'] = 1
    tks_group_6hours = ex_tickers.groupby(['tickers', pd.Grouper(key='created', freq=f'{time_mins}Min')])
    timed_df = pd.concat([tks_group_6hours['count'].count(), tks_group_6hours['sentiment'].mean(), tks_group_6hours['score'].sum()], axis=1)
    return timed_df.astype(np.float32)
