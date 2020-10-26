from apiclient.discovery import build 
from airflow.models import Variable
from datetime import datetime
import csv

import os, sys
sys.path.insert(0, os.path.dirname(__file__))
import google_sheet


def video_statistics(youtube_object, video_id):
    """
    Returns youtube video statistics.
    :param youtube_object: Youtube Client.
    :param video_id: Youtube video id.
    :return: Dict of statistics.
    """
    video_info = youtube_object.videos().list(part="statistics", id=video_id).execute()
    return video_info['items'][0]


def search_keyword(youtube_object, query, max_results):
    """
    Search youtube videos for given keyword and
    gets their metrics and writes to google sheet
    :param client: Google Sheet Client.
    :param youtube_object: Youtube Client.
    :param query: Keyword for search.
    :param max_results: max results to collect.
    """
    search_keyword = youtube_object.search().list(q = query, part = "id, snippet",
                                               maxResults = max_results).execute()

    # extracting the results from search response
    results = search_keyword.get("items", [])
    return results


def collect_data(**kwargs):
    """
    Initialize youtube client and start collection
    Gets params from previous task
    :param kwargs: to get params from previous tasks
    """
    task_instance = kwargs['task_instance']
    # get config passed from previous task
    config = task_instance.xcom_pull(task_ids='get_config', key='config')
    print(config)

    DEVELOPER_KEY = Variable.get("YOUTUBE_API_KEY")
    GOOGLE_SHEET_ID = Variable.get("GOOGLE_SHEET_ID")

    # init youtube client
    youtube_object = build("youtube", "v3", developerKey = DEVELOPER_KEY,
                            cache_discovery = False)

    search_query = config.get('search query')
    max_results = config.get('max results')
    worksheet = config.get('output sheet')

    current_time = datetime.now()
    results = search_keyword(youtube_object, search_query, max_results)

    for video in results:
        if video['id']['kind'] != 'youtube#channel':
            snippet = video['snippet']
            video_url = 'https://www.youtube.com/watch?v={}'.format(video['id']['videoId'])

            video_info = video_statistics(youtube_object, video['id']['videoId'])
            statistics = video_info['statistics']

            output = [current_time.strftime("%Y-%m-%d %H:%M:%S"), video['id']['videoId'], snippet['title'],
                      snippet['channelTitle'], video_url, snippet['publishedAt'], statistics['viewCount'],
                      statistics['likeCount'], statistics['dislikeCount'], statistics['commentCount']
            ]

            google_sheet.append_to_sheet(GOOGLE_SHEET_ID, worksheet, output)
