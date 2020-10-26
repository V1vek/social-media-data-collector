from facebook_scraper import get_posts
from airflow.models import Variable
from datetime import datetime
import math

import os, sys
sys.path.insert(0, os.path.dirname(__file__))
import google_sheet


def get_posts_data(**kwargs):
    """
    Collects posts from Facebook page
    Gets config from previous task
    :param kwargs: to get params from previous tasks
    """
    task_instance = kwargs['task_instance']
    # get config passed from previous task
    config = task_instance.xcom_pull(task_ids='get_config', key='config')
    print(config)

    search_term = config.get('search query')
    max_results = config.get('max results')
    worksheet = config.get('output sheet')

    pages = 1 if ((max_results - 2) <= 0) else math.ceil((max_results - 2) / 4) + 1
    current_time = datetime.now()
    GOOGLE_SHEET_ID = Variable.get("GOOGLE_SHEET_ID")

    reactions = ['like', 'love', 'wow', 'haha', 'support', 'anger', 'sorry']

    for post in get_posts(search_term, pages=pages, extra_info=True):
        output = [current_time.strftime("%Y-%m-%d %H:%M:%S"), post['post_id'],
                  post['user_id'], post['time'].strftime("%Y-%m-%d %H:%M:%S"),
                  post['post_text'], post['likes'], post['comments'], post['shares']]

        if 'reactions' in post:
            for reaction in reactions:
                value = post['reactions'][reaction] if (reaction in post['reactions']) else 0
                output.append(value)

        output = output + [post['post_url'], post['link'], post['shared_text']]

        google_sheet.append_to_sheet(GOOGLE_SHEET_ID, worksheet, output)
