# Tickerrain
TickerRain is an open-source web app that stores and analysis Reddit posts in a transparent and semi-interactive manner. 

## Overview

A simple webpage will display the sentimental analysis and entities of the last post processed, then it will display DB info and finally three graphs of the most mentioned tickers in Reddit.

![Web server](/images/web_screenshot.png?raw=true "Web Server Overview")


The graphs are updated every 120 seconds and refreshing the page will display the analysis of a new post.


## Requirements

Python3 and the following packages:

* pandas
* flask
* redis
* cairosvg
* nltk
* spacy
* matplotlib
* asyncpraw
* cachetools

Other than that you need [Cairo](https://cairosvg.org/), for example, for Ubuntu run `apt-get install libpangocairo-1.0-0`.

## Running 
First, make sure you have a Redis DB running.

In the file `substoscrap.txt` specify what subreddits to analyze.
 
There are 3 parts, a process to get the submissions and store them in Redis DB, one to process them, and then finally one to run the webserver.


#### Getting Submissions

Run `python news.py <client_id> <client_secrets>` with arguments the crendentials for your account reddit API, see more [here](https://praw.readthedocs.io/en/latest/getting_started/authentication.html#oauth).

This will start getting posts, comments, and Redditors from Reddit and store them in Redis DB.

#### Processing Posts

Run `python -m spacy download en_core_web_lg`to get spacy-model required for processing posts.

Run `python process.py`, this will connect to the DB and start calculating metrics every 120 seconds, the results will be stored in 3 files, `tickers_df_<days>.p`.

The metrics computed right now are:
* Mentions -> Detectes what ticker is being talked about and counts the total mentions of it.
* Score -> Calculates the log score which takes into account the upvotes and downvotes.
* Sentiment -> Using Spacy Vader sentimental analysis it aggregates the general sentiment about the ticker.

#### Flask Web Server

Run `python flask_example.py` to start the webserver that displays the results, DB infos and the last post being processed.
Access it by opening a browser and going to 127.0.0.1:5000

## Issues and TODO

Currently, the processing code, using Pandas, needs to be optimized, it needs to use Pandas in a better way.
The ticker detection needs to be improved, it emits warnings and misses some.

- [ ] Improve ticker detecting, combining Spacy entities.
- [ ] Optimize Pandas processing.
- [ ] Add more metrics.
- [ ] Improve the design of the Web page.
- [ ] Auto download of tickers.csv from NASDAQ.
