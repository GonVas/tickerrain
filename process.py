import math
import re
import asyncio
from functools import lru_cache, partial
import time
from datetime import datetime
from dateutil import tz
import datetime
from multiprocessing import  Pool
import gzip
import pickle

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

r = redis.Redis(
host='localhost',
port=6379,)

r = redis.Redis(db=8)

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def parallelize(data, func, num_of_processes=2):
    data_split = np.array_split(data, num_of_processes)
    pool = Pool(num_of_processes)
    data = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data

def run_on_subset(func, data_subset):
    return data_subset.apply(func, axis=1)

def parallelize_on_rows(data, func, num_of_processes=2):
    return parallelize(data, partial(run_on_subset, func), num_of_processes)



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



def create_pandas_redis():
    data_all = []
    cursor = 0
    i = 0
    while(True):
        cursor, posts_ret = r.scan(cursor, "submi:*", 100)
        for post in posts_ret:
            post_content = r.hgetall(post)

            post_id = post
            post_score = post_content[b'score'].decode("utf-8")
            post_title = post_content[b'title'].decode("utf-8")
            post_body = post_title + " " + post_content[b'body'].decode("utf-8")
            post_created = post_content[b'created'].decode("utf-8")

            data_all.append([post_id, post_score, post_body, post_title, post_created])

        if(cursor == 0 or i > 200):
            break

    df = pd.DataFrame(data_all, columns=['id', 'score', 'body', 'title', 'created']) 
    return df



def process_tickers(text):
    tickers_ment = set([word.split("$")[-1] for word in text.split() if word.startswith('$')])

    text = re.sub('[^a-zA-Z0-9 \n\.]', ' ', text)
    text = [' '.join(filter(str.isupper, word.split())) for word in text.split()]
    tickers_ment = tickers_ment.union(set([word for word in text]))

    tickers_ment = tickers_ment.intersection(set(get_ticker_list()))

    list_tickers = [ticker.upper() for ticker in tickers_ment] 

    return list_tickers

def p_tickers(row):
    return process_tickers(row.body)


nlp = spacy.load("en_core_web_lg")

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

def sum_(x):
    return sum(x)


def calculate_df(df):
    data_df = df.filter(['tickers', 'score', 'sentiment', 'created'])

    data_df['score'] = data_df.score.astype(np.int64)
    data_df['created'] = data_df.created.astype(np.float32)
    ex_tickers = data_df.explode('tickers').dropna()
    ex_tickers['count'] = 1
    df_grouped = ex_tickers.groupby('tickers')

    df_count = df_grouped['count'].count()
    df_score_mean = df_grouped['score'].mean()
    df_sentiment_mean = df_grouped['sentiment'].apply(lambda vals: sum([val['compound'] for val in vals])/len(vals)).sort_values(ascending=False)
    return pd.concat([df_count, df_score_mean, df_sentiment_mean], axis=1).sort_values(by='count', ascending=False)


#def deb_func(row):

def calculate_df_timed(df, time_mins=360):
    data_df = df.filter(['tickers', 'score', 'sentiment', 'created'])
    data_df['score'] = data_df.score.astype(np.int32)
    data_df['created'] = data_df['created'].astype(np.float32)
    data_df['created'] = data_df['created'].apply(lambda created: pd.Timestamp(created, unit='s')) 

    ex_tickers = data_df.explode('tickers').dropna()
    ex_tickers['sentiment'] = ex_tickers['sentiment'].apply(lambda row: row['compound'])
    ex_tickers['count'] = 1
    
    tks_group_6hours = ex_tickers.groupby(['tickers', pd.Grouper(key='created', freq=f'{time_mins}Min')])
    
    timed_df = pd.concat([tks_group_6hours['count'].count(), tks_group_6hours['sentiment'].mean(), tks_group_6hours['score'].sum()], axis=1)
    timed_df = timed_df.unstack(1, fill_value=0).stack(level=1)
    return timed_df.astype(np.float32)


def sentiment(row, ret_doc=False):
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

    print('Processed another one')


    if(ret_doc == False):
        return doc.ents, sentiment
    else:
        return doc, sentiment



def day_filter(row, day=1):
    now = time.time()
    if(now - float(row['created']) < day*86_400):
        return True
    else:
        return False


def plot_df(df, tickers_numb=7, tail_numb=100):
    
    df_count_sum = df.groupby('tickers')['count'].cumsum()
    l0_index = df_count_sum.sort_values(ascending=False).index.get_level_values(0).unique()[0:tickers_numb]
    
    df_count_sum = df_count_sum[l0_index]
    df_count_bar = df['count'][l0_index]
    df_count_sum = df_count_sum.groupby(level=0).tail(tail_numb)

    df_graph_df = df_count_sum.reset_index().pivot('created', 'tickers', 'count')
    
    fig, ax = plt.subplots()

    dates = mdates.drange(df_graph_df.index[0], df_graph_df.index[-1], datetime.timedelta(minutes=30))

    df_graph_df_diff = df_graph_df.diff()
    """
    amount_ticks = min(5, tickers_numb)
    width = 1/amount_ticks
    colors = ['red', 'green', 'blue', 'yellow', 'purple']
    plots = []
    for idx, col in enumerate(df_graph_df_diff.columns):
        if(idx >= amount_ticks):
            break
        idxx = idx - int(amount_ticks/2)
        plots.append(ax.bar(np.arange(20)+width*idxx  , df_graph_df_diff[col].values, align='center', width=width, color=colors[idxx], label=str(col)))
    """
    #ax.set(xticks=np.arange(20), xticklabels=df_graph_df.index) #Same as plt.xticks

    #fig.autofmt_xdate()
    #plt.legend(handles=plots)
    #df_graph_df.plot()
    #ax.plot(df_graph_df, label=df_graph_df.columns)
    df_graph_df.plot(ax=ax)

    return fig


def processed_df():
    df = create_pandas_redis()

    print('Processing -> Getting Tickers')
    df['tickers'] = parallelize_on_rows(df, p_tickers)
    
    print('Processing -> NLP processing')
    df["ents"], df["sentiment"] = zip(*df.apply(nlp_processing, axis=1))
    #df_nlps = parallelize_on_rows(df, nlp_processing)
    #df["ents"], df["sentiment"] = zip(*df_nlps)

    df_3, df_1 = df[df.apply(partial(day_filter, day=3), axis=1)], df[df.apply(partial(day_filter, day=1), axis=1)]

    print('Processing -> Calculating for each ticker')
    timed_df = calculate_df_timed(df, time_mins=30)
    plot_df(timed_df)
    return calculate_df(df), calculate_df(df_3), calculate_df(df_1), df




async def processing_last():
    while True:
        last_processed, last_processed_3, last_processed_1, all_df = processed_df()
        print('Processing -> Storing processed Files')
        last_processed.to_pickle('tickers_df_7.p')
        last_processed_3.to_pickle('tickers_df_3.p')
        last_processed_1.to_pickle('tickers_df_1.p')

        all_df.to_pickle('all_df.p')
        #all_df.to_csv("all_df.csv.gz", compression="gzip")
        print('Finished Processing awaiting 120secs')
        await asyncio.sleep(120)
        


if __name__ == '__main__':
    #processed_df()
    asyncio.run(processing_last())
