import math
import io
import redis
import time
import random
import functools

from datetime import datetime
from dateutil import tz
import datetime

from flask import Flask
import pandas as pd
import spacy
import nltk
import cachetools.func
nltk.download('vader')
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer

from cairosvg import svg2png
from flask import Response
from flask import send_from_directory
from flask import Markup
from flask import render_template
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
plt.style.use('seaborn')

import process


r = redis.Redis(
host='redis',
port=6379,db=8)

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
    p_creator = None
    p_created = None

    if(b'comment_body' in post):
        p_type = "comment"
        p_body = str(post[b'comment_body'].decode("utf-8"))
        p_score = str(post[b'comment_score'])
        p_creator = str(post[b'author'].decode("utf-8"))
        p_created = str(post[b'comment_created'].decode("utf-8"))
    else:
        p_title = str(post[b'title'])
        p_score = str(post[b'score'])
        p_body = str(post[b'title'].decode("utf-8") + " . " + post[b'body'].decode("utf-8"))
        p_creator = str(post[b'author'].decode("utf-8"))
        p_created = str(post[b'created'].decode("utf-8"))


    return {"type":p_type,
            'title': p_title,
            'body':p_body,
            'process_body':process_body,
            'tickers':tickers_ment,
            'score':p_score,
            'author':p_creator,
            'created':p_created
            }


def create_figure(day=7):
    fig = plt.figure()


    fig.suptitle(f'Ticker Mentions, Score (Upvotes & Downvotes) and Sentiment (Last {day} days)')

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
    ticks_s, t_score = list(scores.index), [math.log(score) - 2 for score in scores]#list(scores)
    ticks_sen, t_sen = list(sents.index), [math.log(sents) - 2 for sents in sents]#list(sents)

    ax1.bar(ticks_c, t_counts)

    ax2.bar(ticks_s, t_score)

    ax3.bar(ticks_sen, t_sen)


    plt.subplots_adjust(top=0.92, bottom=0.08, left=0.10, right=0.95, hspace=0.25,
                    wspace=0.35)


    return fig


def html_last_sent():
    nlp = spacy.load("en_core_web_sm")
    processed_sentence = get_last_process()
    processed_sentence['body'] = processed_sentence['body'].strip().replace("\n", "  ")

    author = processed_sentence['author']
    date = str(datetime.datetime.fromtimestamp(int(processed_sentence['created'].split(".")[0])))

    #TODO many calls to this func
    tickers = process.process_tickers(processed_sentence['body'].strip())

    doc, sentiment = process.sentiment(processed_sentence, ret_doc=True)

    if(doc == None):
        doc = nlp(processed_sentence['body'].strip().replace("\n", "  "))
        sid = SentimentIntensityAnalyzer()
        sentiment = sid.polarity_scores(doc.text)

    svg = spacy.displacy.render(doc, style="ent", jupyter=False)

    return svg, sentiment, tickers, author, date



@app.route('/plot<numb>.png')
def plot_png(numb):
    fig = create_figure(day=numb)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')


@app.route("/styles.css")
def styles():
    return send_from_directory("templates", "styles.css")


@cachetools.func.ttl_cache(maxsize=128, ttl=10)
def redis_db_info():
    info = r.info()
    dict_info = {}
    dict_info['Submissions/Posts'] = info['db8']['keys']
    dict_info['mem_used'] = info['used_memory_human']
    dict_info['clients_connected'] = info['connected_clients']
    dict_info['uptime_hours'] = str(round(int(info['uptime_in_seconds'])/3600, 3))

    return dict_info


@app.route('/')
def index():
    svg, sentiment_post, tickers, author, date = html_last_sent()
    info = redis_db_info()
    return render_template('index.html',  last_sent_html=Markup(svg), sentiment=sentiment_post, tickers=tickers, author=author, date=date, info=info)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
