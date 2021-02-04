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


r = redis.Redis(
host='localhost',
port=6379,)

r = redis.Redis(db=6)


#options = {"compact": True, "bg": "#09a3d5",
#           "color": "white", "font": "Source Sans Pro"}

text = "When Sebastian Thrun started working on self-driving cars at Apple in 2007, few people outside of the company took him seriously."

nlp = spacy.load("en_core_web_lg")
doc = nlp(text)
displacy.serve(doc, style="ent")
#displacy.serve(doc, style="dep", options=options)


#import pudb; pudb.set_trace()


