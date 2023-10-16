# pylint: disable=import-error
"""Class for scarping data fro reddit using paraw
"""
import os
from typing import List, Union
from dataclasses import dataclass
import praw
from loggers import logging
from src.utils.utils import TimeUtils


@dataclass
class RedditAuth:
    """Represents authentication details for Reddit."""
    client_id: str = ''
    client_secret: str = ''
    user_agent: str = ''
    redirect_url: str = ''

@dataclass
class Comment:
    """Represents a comment on a Reddit post."""
    comment_id: str = ''
    post_id: str = ''
    body: str = ''
    author: str = ''
    score: int = 0
    permalink: str = ''
    created_utc: float = 0.0

@dataclass
class Post:
    """Represents a Reddit post."""
    post_id: str = ''
    title: str = ''
    author: str = ''
    subreddit: str = ''
    score: int = 0
    upvote_ratio: float = 0.0
    num_comments: int = 0
    permalink: str = ''
    created_utc: float = 0.0
    url: str = ''
    selftext: str = ''
    subscribers: int = 0
    over_18: bool = False
    link_flair_richtext: str = ''
    subreddit_name_prefixed: str = ''
    name: str = ''
    subreddit_type: str = ''
    ups: int = 0
    author_premium: bool = False


@dataclass
class RedditSearch:
    """Represents a filter
    """
    subreddit: str = ''
    limit: Union[int, str] = None
    time_filter: str = ''


class RedditScraper:
    """
    A class for scraping data from Reddit.
    """

    def __init__(self, redditautho: RedditAuth) -> None:
        """Create a new RedditScraper instance."""
        self._reddit = praw.Reddit(
            client_id=os.getenv(redditautho.client_id),
            client_secret=os.getenv(redditautho.client_secret),
            user_agent=os.getenv(redditautho.user_agent),
            redirect_url=os.getenv(redditautho.redirect_url)
        )
        logging.info("Scapping in Reddit is started")

    def get_posts(self, search_filter:RedditSearch) -> List[Post]:
        """
        Get the top posts from a given subreddit based on the specified filter.

        Args:
            search_filter (RedditSearch): The RedditSearch object representing the search filter.

        Returns:
            List[Post]: A list of Post objects, each representing a single post on the subreddit.

        Example:
            - reddit_search = RedditSearch(subreddit='python', limit=10)
            - redditautho = {'client_id': 'ddd', 'client_secret': 'dd'}
            - scraper = RedditScraper(**redditautho)
            - posts = scraper.get_posts(search_filter=reddit_search)
        """
        logging.info("scaping posts is started")
        intvl = TimeUtils.get_time_interval(search_filter.time_filter)
        logging.info(f"Posts for this period {search_filter.time_filter} in this interval {intvl}")
        posts = []
        for post in self._reddit.subreddit(search_filter.subreddit)\
                    .top(time_filter=search_filter.time_filter, limit=search_filter.limit):

            if not intvl[0] <= TimeUtils.seconds_to_datetime(post.created_utc) <= intvl[1]:
                continue

            post_dict = Post(
                post_id=post.id,
                title=post.title,
                author=post.author.name if post.author else None,
                subreddit=post.subreddit.display_name if post.subreddit else None,
                score=post.score,
                upvote_ratio=post.upvote_ratio,
                num_comments=post.num_comments,
                permalink=post.permalink,
                created_utc=post.created_utc,
                url=post.url,
                selftext=post.selftext,
                subscribers=post.subreddit_subscribers,
                over_18=post.over_18,
                link_flair_richtext=post.link_flair_richtext,
                subreddit_name_prefixed=post.subreddit_name_prefixed,
                name=post.name,
                subreddit_type=post.subreddit_type,
                ups=post.ups,
                author_premium=post.author_premium
            )
            posts.append(post_dict)
        logging.info("finished getting posts")
        return posts

    def get_comments(self, post_id: str) -> List[Comment]:
        """
        Get all comments from a given post.

        Args:
            post_id (str): The ID of the post to scrape comments from.

        Returns:
            List[Comment]: A list of Comment objects, each representing a single comment on the post

        Example:
            get_comments("abc123")
        """
        logging.info(f"scaping comments is started for post {post_id}")
        comments = []
        submission = self._reddit.submission(id=post_id)
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            comment_dict = Comment(
                comment_id=comment.id,
                post_id=post_id,
                body=comment.body,
                author=comment.author.name if comment.author else None,
                score=comment.score,
                permalink=comment.permalink,
                created_utc=comment.created_utc
            )
            comments.append(comment_dict)
        logging.info(f"finished getting comments for post {post_id}")
        return comments

    def get_all_comments(self, post_ids: List[str]) -> List[Comment]:
        """
        Get all comments for each post.

        Args:
            post_ids (List[str]): A list of post IDs.

        Returns:
            List[Comment]: A list of Comment objects representing comments from each post.

        Example:
            get_all_comments(["post1", "post2", "post3"])
        """
        return [comment for post_id in post_ids for comment in self.get_comments(post_id)]
