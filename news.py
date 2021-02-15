import re
import math
import asyncio
import time
import functools

import sys

from datetime import datetime
from dateutil import tz
import datetime

import redis
import asyncpraw
import pandas as pd



def float_to_datetime(fl):
    return datetime.datetime.fromtimestamp(fl)


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


async_id = 0


r = redis.Redis(
host='redis',
port=6379,db=8)

def add_to_redis(data_dic):
    with r.pipeline() as pipe:
        for s_id, submi in data_dic.items():
            if(r.exists(s_id)):
                continue
            else:
                pipe.hmset(s_id, submi)
                pipe.execute()

    r.save()

def add_to_redis_id(content_id, items):
    with r.pipeline() as pipe:
        if(r.exists(content_id)):
            print(f"Exists {content_id}")
        else:
            pipe.hmset(content_id, items)
            if("author" not in content_id):
                pipe.lpush("last10keys", content_id)
                pipe.ltrim("last10keys", 0, 9)
            pipe.execute()

    r.save()


async def get_posts(sub, c_id, c_secrets,username, password, store_csv=False, async_id=0, sort_type="hot", process_func=None, ticker_func=None):

    reddit = asyncpraw.Reddit(client_id=c_id,     
                         client_secret=c_secrets,  
                         user_agent="Scrapping Reddit", 
                         username = username,     
                         password = password)     

    award_val = 5
    posts_indexed = 0

    subreddit = await reddit.subreddit(sub)   # Chosing the subreddit

    posts_dict = {}
    authors_id = {}

    if(sort_type == "hot"):
        async_for_func = functools.partial(subreddit.hot, limit=300) #subreddit.hot(limit=None)
    else:
        async_for_func = subreddit.stream.submissions

    try:
        async for submission in async_for_func():
            if(submission.id not in posts_dict):
                post_contents = {}
                post_contents["title"] = submission.title
                post_contents["score"] = submission.score + award_val*len(submission.all_awardings)
                post_contents["url"] = submission.url
                post_contents["comms_num"] = submission.num_comments
                post_contents["created"] = submission.created
                post_contents["body"] = submission.selftext

                all_text = submission.title + " " + submission.selftext

                post_contents["process_body"] = "None"
                post_contents["tickers_ment"] = "None"

                if(process_func != None):
                    post_contents["process_body"] = process_func(all_text)

                if(ticker_func != None):
                    post_contents["tickers_ment"] = ticker_func(all_text)


                if(submission.author == None or submission.author == "deleted"):
                    post_contents["author"] = "deleted"
                else:
                    post_contents["author"] = submission.author.name

                authors_content = {}

                sub_author = asyncpraw.models.Redditor(reddit, submission.author.name)
                
                await sub_author.load()

                authors_content["id"] = sub_author.id
                authors_content["name"] = sub_author.name
                authors_content["creator"] = sub_author.created
                authors_content["comment_karma"] = sub_author.comment_karma
                authors_content["posted"] = submission.id

                dt = float_to_datetime(submission.created)
                #print('Created: {} id: {}, Post: "{}".'.format(dt, f"submi:{submission.id}", (submission.title[:40] + '..') if len(submission.title) > 40 else submission.title))

                posts_dict[f"submi:{submission.id}"] = post_contents

                add_to_redis_id(f"submi:{submission.id}", post_contents)
                add_to_redis_id(f"author:{sub_author.name}", authors_content)

                posts_indexed += 1

                comments_dict = {}
                
                try:
                    comments = await submission.comments()
                    await comments.replace_more(limit=0)
                    all_comments = await comments.list()

                    if(len(all_comments) > 10):
                        all_comments = all_comments[0:9]

                    for comment in all_comments:
                        if(comment.id not in comments_dict):
                            comments_contents = {}
                            comments_contents["comment_id"] = comment.id
                            comments_contents["comment_parent_id"] = comment.parent_id
                            comments_contents["comment_body"] = comment.body
                            comments_contents["comment_created"] = comment.created
                            comments_contents["comment_link_id"] = comment.link_id
                            comments_contents["comment_score"] = comment.score + award_val*len(comment.all_awardings)
                            
                            comments_contents["process_body"] = "None"
                            comments_contents["tickers_ment"] = "None"

                            if(process_func != None):
                                comments_contents["process_body"] = process_func(comment.body)

                            if(ticker_func != None):
                                comments_contents["tickers_ment"] = ticker_func(comment.body)

                            if(comment.author == None or comment.author == "deleted"):
                                comments_contents["author"] = "deleted"
                            else:
                                comments_contents["author"] = comment.author.name

                            #print('Comment Storing: "{}", score: {} created: {} id: {}'.format((comment.body[:40] + '..') if len(comment.body) > 40 else comment.body, comments_contents["comment_score"], comment.created, comment.id))
                            dtc = float_to_datetime(comment.created)
                            #print('Created: {} id: {}, Post: "{}".'.format(dtc, f"comment:{comment.id}", (comment.body[:40] + '..') if len(comment.body) > 40 else comment.body))

                            authors_content = {}

                            try:
                                sub_author = asyncpraw.models.Redditor(reddit, comment.author.name)
                                
                                await sub_author.load()

                                authors_content["id"] = sub_author.id
                                authors_content["name"] = sub_author.name
                                authors_content["creator"] = sub_author.created
                                authors_content["comment_karma"] = sub_author.comment_karma
                                authors_content["posted"] = comment.id

                                add_to_redis_id(f"author:{sub_author.name}", authors_content)

                            except Exception as e:
                                print('Got Comment Author exception, deleted author')
                                #print(e)

                            comments_dict[f"comment:{comment.id}"] = comments_contents   
                            add_to_redis_id(f"comment:{comment.id}", comments_contents)


                    post_contents["comments"] = comments_dict

                    posts_dict[f"submi:{submission.id}"] = post_contents
                    print('Thread: {}. Number of posts indexed: {}'.format(async_id, len(posts_dict)))

                except Exception as e:
                    print('Got Comment exception:')
                    print(e)


    except Exception as e_sub:
        print('Got Submission exception:')
        print(e_sub)
            

        print('Thread done for some reason')

    post_data = pd.DataFrame(posts_dict).T

    if(store_csv):
        post_data.to_csv(sub + "_subreddit.csv")

    return post_data


async def get_stream_posts(sub, c_id, c_secrets,  async_id=0):

    reddit = asyncpraw.Reddit(client_id=c_id,      # your client id
                         client_secret=c_secrets,  #your client secret
                         user_agent="Scrapping Reddit", #user agent name
                         username = "PM_ME_YOUR_OTC_PENNY",     # your reddit username
                         password = "qm-3cFZ@")     # your reddit password


    subreddit = await reddit.subreddit("AskReddit")

    async for submission in subreddit.stream.submissions():
        dt = float_to_datetime(submission.created)
        print('Created: {} id: {}, Post: "{}".'.format(dt, submission.id, (submission.title[:40] + '..') if len(submission.title) > 40 else submission.title))

    print('Done for')


def list_mentions(row):

    text = row.body + row.title
    tickers_ment = set([word.split("$")[-1] for word in text.split() if word.startswith('$')])

    text = re.sub('[^a-zA-Z0-9 \n\.]', ' ', text)
    text = [' '.join(filter(str.isupper, word.split())) for word in text.split()]
    tickers_ment = tickers_ment.union(set([word for word in text]))

    tickers_ment = tickers_ment.intersection(set(tickers))

    return [ticker.upper() for ticker in tickers_ment]   


def process_tickers(text):
    tickers_ment = set([word.split("$")[-1] for word in text.split() if word.startswith('$')])

    text = re.sub('[^a-zA-Z0-9 \n\.]', ' ', text)
    text = [' '.join(filter(str.isupper, word.split())) for word in text.split()]
    tickers_ment = tickers_ment.union(set([word for word in text]))

    tickers_ment = tickers_ment.intersection(set(tickers))

    list_tickers = [ticker.upper() for ticker in tickers_ment] 

    print(';'.join(list_tickers))

    return ';'.join(list_tickers)
    

def test_data(text):
    tickers_ment = set([word.split("$")[-1] for word in text.split() if word.startswith('$')])

    text = re.sub('[^a-zA-Z0-9 \n\.]', ' ', text)
    text = [' '.join(filter(str.isupper, word.split())) for word in text.split()]
    #tickers_ment = list(set([word for word in text.split(" ")]).intersection(set(tickers)))
    tickers_ment = tickers_ment.union(set([word for word in text]))

    tickers_ment = tickers_ment.intersection(set(tickers))

    return [ticker.upper() for ticker in tickers_ment]   



async def reddit_get_subs(subs, c_id, c_secret, username, password):
    res = await asyncio.gather(*(get_posts(sub, c_id, c_secret, username, password, async_id=idx, ticker_func=process_tickers) for idx, sub in enumerate(subs)))
    return list(res)



if __name__ == '__main__':

    print(sys.argv)

    if(len(sys.argv) != 5):
        print("Wrong number of arguments, python news.py <client_id> <client_secret> <username> <password>")
    else:
    
        client_id = sys.argv[1]
        client_secret = sys.argv[2]
        username = sys.argv[3]
        password = sys.argv[4]
        
        subs = []

        with open('substoscrap.txt') as f:
            subs = [line.rstrip() for line in f]
        
        results = asyncio.run(reddit_get_subs(subs, client_id, client_secret, username, password))


