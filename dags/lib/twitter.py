from datetime import datetime, timedelta
from airflow.models import Variable
import tweepy as tw
import twint

import os, sys
sys.path.insert(0, os.path.dirname(__file__))
import google_sheet


consumer_key = Variable.get('TWITTER_CONSUMER_KEY')
consumer_secret = Variable.get('TWITTER_CONSUMER_SECRET')
access_token = Variable.get('TWITTER_ACCESS_TOKEN')
access_token_secret = Variable.get('TWITTER_ACCESS_TOKEN_SECRET')

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)


def search_tweets(query, since_date, max_results, worksheet):
    """
    Uses Tweepy
    Search tweets for a keyword and writes
    results into google sheet
    :param query: Search query.
    :param since_date: Since date.
    :param max_results: max results to be retrieved.
    :param worksheet: output worksheet.
    :return: Conversations - List of tweet ids and user ids.
    """
    current_time = datetime.now()
    conversations = []
    GOOGLE_SHEET_ID = Variable.get("GOOGLE_SHEET_ID")

    tweets_cursor = tw.Cursor(api.search, q=query, lang="en",
                              since=since_date,
                              tweet_mode='extended').items(max_results)

    for tweet in tweets_cursor:
        output = [current_time.strftime("%Y-%m-%d %H:%M:%S"), tweet.id_str, tweet.user.screen_name,
                  tweet.user.name, tweet.created_at.strftime("%Y-%m-%d %H:%M:%S"), tweet.full_text,
                  tweet.retweet_count, tweet.favorite_count, tweet.source, tweet.user.location,
                  tweet.user.created_at.strftime("%Y-%m-%d %H:%M:%S")]

        google_sheet.append_to_sheet(GOOGLE_SHEET_ID, worksheet, output)
        conversations.append({
            'username': tweet.user.screen_name,
            'tweet_id': tweet.id,
            'created_at': tweet.created_at
        })

    return conversations


def collect_data(**kwargs):
    """
    Uses Tweepy
    Collect tweets based on given keywords
    Gets params from previous task
    :param kwargs: to get params from previous tasks
    """
    task_instance = kwargs['task_instance']
    # get config passed from previous task
    config = task_instance.xcom_pull(task_ids='get_config', key='config')
    print(config)

    search_query = config.get('search query')
    since_date = (datetime.today() - timedelta(days = 1)).strftime('%Y%m%d')

    if config.get('max results'):
        max_results = config.get('max results')

    worksheet = config.get('output sheet')
    conversations = search_tweets(search_query, since_date, max_results, worksheet)
    task_instance.xcom_push(key='conversations', value=conversations)


def collect_replies(**kwargs):
    """
    Uses Tweepy
    Collect all replies of tweets by going through all the
    mentions of the author of the tweet and matching with
    the in reply to status id value
    Gets conversations list from previous task
    :param kwargs: to get params from previous tasks
    """
    task_instance = kwargs['task_instance']
    conversations = task_instance.xcom_pull(task_ids='collect_data', key='conversations')

    for conversation in conversations:
        query = 'to:@{}'.format(conversation['username'])
        tweets_cursor = tw.Cursor(api.search, q=query, lang="en",
                              since=conversation['created_at'],
                              tweet_mode='extended').items()

        for tweet in tweets_cursor:
            if tweet.in_reply_to_status_id == conversation['tweet_id']:
                print(tweet.full_text, tweet.created_at, tweet.user.screen_name, tweet.user.name)


def collect_tweets_history(**kwargs):
    """
    Uses Twint
    Collect tweet history for a given keyword and time range
    Gets config from previous task
    :param kwargs: to get params from previous tasks
    """
    task_instance = kwargs['task_instance']
    # get config passed from previous task
    config = task_instance.xcom_pull(task_ids='get_config', key='config')
    print(config)

    c = twint.Config()

    c.Search = config.get('search query')
    c.Since = config.get('since')

    if config.get('max results'):
        c.Limit = config.get('max results')

    c.Store_object = True
    c.Custom["user"] = ["id", "tweet", "user_id", "username", "hashtags", "mentions"]
    c.User_full = True
    c.Hide_output = True

    twint.run.Search(c)
    tweets = twint.output.tweets_list

    GOOGLE_SHEET_ID = Variable.get("GOOGLE_SHEET_ID")
    worksheet = config.get('output sheet')
    current_time = datetime.now()

    for tweet in tweets:
        output = [current_time.strftime("%Y-%m-%d %H:%M:%S"), tweet.id_str, tweet.username,
                  tweet.name, "{} {}".format(tweet.datestamp, tweet.timestamp), tweet.tweet,
                  tweet.retweets_count, tweet.likes_count, tweet.source, tweet.place, None]
        google_sheet.append_to_sheet(GOOGLE_SHEET_ID, worksheet, output)