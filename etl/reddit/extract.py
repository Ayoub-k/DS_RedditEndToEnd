"""Extract data from Reddit
"""

import pandas as pd
from config import Config
from reddit import RedditScraper, RedditAuth, RedditSearch
from loggers import logging
from paths import Paths
from datetime import datetime
import os


def save_data(data, data_name, folder_name, pattern, extension=".parquet"):
    data_frame = pd.DataFrame((vars(elm) for elm in data))
    file_name = f"{datetime.now().strftime(pattern)}_{data_name}{extension}"
    file_path = Paths.get_project_root_path_from_env() / folder_name / file_name
    data_frame.to_parquet(str(file_path), compression='snappy')

if __name__ == '__main__':
    try:
        Config.load_config()
        logging.info("Extracting data from Reddit. The process is started")

        # Load configuration from config.yml
        config = Config.get_config_yaml(Paths.get_project_root_path_from_env() / "config" / "config.yml")

        # Create RedditScraper
        reddit = RedditScraper(RedditAuth(**config['reddit']))

        # Save posts
        posts = reddit.get_posts(RedditSearch(**config['reddit_search']))
        if len(posts) > 0:
            save_data(posts, "posts", "data", config["file_format"])
            logging.info(f"Saved posts. Number of posts: {len(posts)}")

            # Save comments
            comments = reddit.get_all_comments([post.post_id for post in posts])
            if len(comments) > 0:
                save_data(comments, "comments", "data", config["file_format"])
                logging.info(f"Saved comments. Number of comments: {len(comments)}")
            # Move data from local to GCS bucket
        
            logging.info(f"Copying data from VM to GCS: {config['data_folder_bucket']}")
            os.system(f"gsutil mv {Paths.get_project_root_path_from_env()}/data/*.parquet {config['data_folder_bucket']}")

        logging.info("Extracting data from Reddit. The process is finished")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
