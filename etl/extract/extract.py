"""Extract data from Reddit
"""

import pandas as pd
from src.common.utils.config import Config
from src.common.utils.reddit import RedditScraper, RedditAuth, RedditSearch


if __name__ == '__main__':
    Config.load_config()

    # load configuration from config.yml
    config = Config.get_config_yml()
    # Create redditScraper
    reddit = RedditScraper(RedditAuth(**config['reddit']))
    # Get posts
    posts = reddit.get_posts(RedditSearch(**config['reddit_search']))
    # Get comments
    #comments = reddit.get_all_comments([post.post_id for post in posts])
    # dataframe comments
    #df_comments = pd.DataFrame((vars(comment) for comment in comments))
    # dataframe posts
    df_posts = pd.DataFrame((vars(post) for post in posts))
    print(df_posts.info())
    