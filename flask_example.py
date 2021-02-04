from flask import Flask


import asyncpraw
import pandas as pd

import io
import requests
import json

import re

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
import nltk
nltk.download('vader')
nltk.download('vader_lexicon')

from nltk.sentiment.vader import SentimentIntensityAnalyzer


from spacy import displacy
from pathlib import Path

from cairosvg import svg2png



import io
import random
from flask import Response
from flask import Markup
from flask import render_template
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt

r = redis.Redis(
host='localhost',
port=6379,)

r = redis.Redis(db=6)


app = Flask(__name__)

processed_posts = []

#cursor, posts_ret = r.scan(0, "submi:*", 100)
#processed_posts += [str(post_ret) for post_ret in posts_ret]

all_mets = {}

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
                    all_mets[tick] += score
                else:
                    all_mets[tick] = score

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


for key, val in all_mets.items():
    all_mets[key] = math.log(val+0.01)/math.log(10)

all_mets = {k: v for k, v in sorted(all_mets.items(), key=lambda item: item[1])}

last_process_idx = 5

@app.route("/get_last_process")
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


def create_csv():
    while(True):
        cursor, posts_ret = r.scan(cursor, "submi:*", 100)
        for post in posts_ret:
            tickers_ment = r.hget(post, "tickers_ment").decode("utf-8") 
            if(tickers_ment != ""):
                score = max(int(r.hget(post, "score")), 1)
                #score = 0.1 + math.log(score+0.1)/math.log(10)
                for tick in tickers_ment.split(';'):
                    if(tick in all_mets):
                        all_mets[tick] += score
                    else:
                        all_mets[tick] = score

        if(cursor == 0):
            break







def create_figure():
    fig = Figure()
    axis = fig.add_subplot(1, 1, 1)
    #xs = range(100)
    #ys = [random.randint(1, 50) for x in xs]

    #plt.bar(*zip(*all_mets.items()))

    keys_ment = list(all_mets.keys())
    keys_ment.reverse()

    vals_ment = list(all_mets.values())
    vals_ment.reverse()

    
    axis.bar(keys_ment[0:10], vals_ment[0:10])
    return fig


@app.route('/last_sent.png')
def plot_last_sent():
    nlp = spacy.load("en_core_web_lg")

    sentence = "Apple is looking at buying U.K. startup for $1 billion"
    doc = nlp(sentence)
    #import pudb; pudb.set_trace()

    svg = displacy.render(doc, style="ent", jupyter=False)

    #svg = displacy.serve(doc, style='ent', jupyter=False)

    output = io.BytesIO()

    svg2png(bytestring=svg, write_to=output)

    return Response(output.getvalue(), mimetype='image/png')


def html_last_sent():
    nlp = spacy.load("en_core_web_lg")

    #sentence = "Apple is looking at buying U.K. startup for $1 billion"

    processed_sentence = get_last_process()

    sentence = processed_sentence['body'].strip()

    tickers_ment = [ticker for ticker in processed_sentence['tickers_ment'].split(';')]

    tk_spans = []

    doc = nlp(sentence)

    sid = SentimentIntensityAnalyzer()
    sentiment = sid.polarity_scores(doc.text)

    
    #import pudb; pudb.set_trace()

    if(tickers_ment != ['']):
        for ticker in tickers_ment:
            pos = doc.text.find(ticker)
            tk_spans.append(doc.char_span(pos, pos + len(ticker), label="ORG"))

    #print(sentence)

    #sentence = parsed_last_sent

    #sp = doc.char_span(doc.text.find('SPY'), doc.text.find('SPY')+len('SPY'), label="ORG")
    doc.ents = list(doc.ents) + tk_spans

    #print(len(doc.ents))
    svg = displacy.render(doc, style="ent", jupyter=False)

    return svg, sentiment



@app.route('/plot.png')
def plot_png():
    fig = create_figure()
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')


@app.route('/')
def hello():
    name = "Hello"
    svg, sentiment_post = html_last_sent()
    return render_template('index.html', title='Welcome', username=name,  last_sent_html=Markup(svg), sentiment=sentiment_post)


if __name__ == '__main__':
    app.run()
