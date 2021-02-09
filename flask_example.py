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

import process

import io
import random
from flask import Response
from flask import Markup
from flask import render_template
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt

plt.style.use('seaborn')

r = redis.Redis(
host='localhost',
port=6379,)

r = redis.Redis(db=7)


app = Flask(__name__)


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
            'tickers':tickers_ment,
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


def create_figure(day=7):
    fig = plt.figure()

    fig.suptitle(f'Ticker Mentions, Score and Sentiment (Last {day} days)')

    ax1 = fig.add_subplot(311)
    ax1.set_ylabel('Mentions')

    ax2 = fig.add_subplot(312)
    ax2.set_ylabel('Log Score')

    ax3 = fig.add_subplot(313)
    ax3.set_ylabel('Log Sentiment')


    pf = pd.read_pickle(f'tickers_df_{7}.p')#process.last_processed

    counts = pf.iloc[0:10, 0]
    scores = pf.iloc[0:10, 1]
    sents = pf.iloc[0:10, 1]

    ticks_c, t_counts = list(counts.index), list(counts)
    ticks_s, t_score = list(scores.index), [math.log(score) for score in scores]#list(scores)
    ticks_sen, t_sen = list(sents.index), [math.log(sents) for sents in sents]#list(sents)

    ax1.bar(ticks_c, t_counts)

    ax2.bar(ticks_s, t_score)

    ax3.bar(ticks_sen, t_sen)


    plt.subplots_adjust(top=0.92, bottom=0.08, left=0.10, right=0.95, hspace=0.25,
                    wspace=0.35)

    return fig


def html_last_sent():
    nlp = spacy.load("en_core_web_lg")
    processed_sentence = get_last_process()

    doc, sentiment = process.sentiment(processed_sentence, ret_doc=True)

    if(doc == None):
        doc = nlp(processed_sentence['body'].strip() + " \n Did Not find any tickers.")
        sid = SentimentIntensityAnalyzer()
        sentiment = sid.polarity_scores(doc.text)

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
    svg, sentiment_post = html_last_sent()
    return render_template('index.html', title='Welcome',  last_sent_html=Markup(svg), sentiment=sentiment_post)


if __name__ == '__main__':
    app.run()
