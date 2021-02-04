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

import io
import random
from flask import Response
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



@app.route("/get_last_process")
def get_last_process():
    last = r.lrange("last10keys", 0, 2)[0]
    post = r.hgetall(last)


    p_type = 'submission'
    tickers_ment = str(post[b'tickers_ment'])
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



@app.route('/plot.png')
def plot_png():
    fig = create_figure()
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')


@app.route('/')
def hello():
    name = "Hello"
    return render_template('index.html', title='Welcome', username=name, last_process_text=get_last_process())


if __name__ == '__main__':
    app.run()
