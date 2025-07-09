from debugpy._vendored.pydevd.pydevd_attach_to_process.winappdbg.win32.peb_teb import CLIENT_ID
import pandas as pd
from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH
#hello


def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    # connecting to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')
    # extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    # transformation
    post_df = transform_data(post_df)
    # loading to csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)

    return file_path

