"""Extract data from Reddit
"""

import pandas as pd
from src.common.utils.config import Config, TimeFormatter
from src.common.utils.reddit import RedditScraper, RedditAuth, RedditSearch
from src.common.utils.s3 import S3BucketConnector
from src.common.constants.constants import DateFormat, FileType


if __name__ == '__main__':
    Config.load_config()

    # load configuration from config.yml
    config = Config.get_config_yml()
    # Create redditScraper
    reddit = RedditScraper(RedditAuth(**config['reddit']))
    # Get posts
    posts = reddit.get_posts(RedditSearch(**config['reddit_search']))
    # Get comments
    comments = reddit.get_all_comments([post.post_id for post in posts])
    # dataframe comments
    df_comments = pd.DataFrame((vars(comment) for comment in comments))
    # dataframe posts
    df_posts = pd.DataFrame((vars(post) for post in posts))
    # laod data to s3 bucket
    s3_bucket_src = S3BucketConnector(
                    access_key=config['s3']['access_key'],
                    secret_key=config['s3']['secret_key'],
                    bucket=config['s3']['src_bucket']
                )
    # format data timenow to str
    DATE_NOW = TimeFormatter.format_dttime_now(
                DateFormat.DATE_FILE_FORMAT.value
            )
    NAME_FILE = f"{DATE_NOW}{FileType.CSV.value}"
    # save dataframe df_comments to s3 bucket in comments folder
    cmt_file = f"{config['folder_bucket']['comment_folder']}/cmt_{NAME_FILE}"
    s3_bucket_src.write_df_to_s3(df_comments, cmt_file)
    # save dataframe df_posts to s3 bucket in posts folder
    cmt_file = f"{config['folder_bucket']['post_folder']}/pst_{NAME_FILE}"
    s3_bucket_src.write_df_to_s3(df_posts, cmt_file)
