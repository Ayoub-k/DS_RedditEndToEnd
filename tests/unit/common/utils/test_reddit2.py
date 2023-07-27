import os
import unittest
from unittest.mock import MagicMock, patch
from  src.common.utils.reddit import RedditScraper, RedditSearch, Post, RedditAuth
import praw
class TestRedditScraper(unittest.TestCase):

    def setUp(self):
        self.reddit_auth = MagicMock(spec=RedditAuth)
        self.reddit_auth.client_id = "your_client_id"
        self.reddit_auth.client_secret = "your_client_secret"
        self.reddit_auth.user_agent = "your_user_agent"
        self.reddit_auth.redirect_url = "your_redirect_url"
        # Creating s3 access keys as environment variables
        os.environ[self.reddit_auth.client_id] = 'KEY1'
        os.environ[self.reddit_auth.client_secret] = 'KEY2'
        os.environ[self.reddit_auth.user_agent] = 'KEY2'
        os.environ[self.reddit_auth.redirect_url] = 'KEY2'
        self.scraper = RedditScraper(self.reddit_auth)

    @patch('praw.Reddit')
    def test_get_posts(self, mock_reddit):
        # Mock the return value of `subreddit().top()` method
        mock_subreddit = mock_reddit().subreddit.return_value
        mock_subreddit.top.return_value = [
            MagicMock(id='post1', title='Title 1', author='Author 1', subreddit='Subreddit 1', score=100),
            MagicMock(id='post2', title='Title 2', author='Author 2', subreddit='Subreddit 2', score=200),
            MagicMock(id='post3', title='Title 3', author='Author 3', subreddit='Subreddit 3', score=300),
        ]

        # Create a RedditSearch object
        search_filter = RedditSearch(subreddit='python', limit=3, time_filter='week')

        # Call the method being tested
        result = self.scraper.get_posts(search_filter)

        # Assertions
        self.assertEqual(len(result), 3)
        self.assertIsInstance(result[0], Post)
        self.assertEqual(result[0].post_id, 'post1')
        self.assertEqual(result[0].title, 'Title 1')
        self.assertEqual(result[0].author, 'Author 1')
        self.assertEqual(result[0].subreddit, 'Subreddit 1')
        self.assertEqual(result[0].score, 100)

        mock_reddit.assert_called_with(
            client_id=self.reddit_auth.client_id,
            client_secret=self.reddit_auth.client_secret,
            user_agent=self.reddit_auth.user_agent,
            redirect_url=self.reddit_auth.redirect_url
        )
        mock_reddit().subreddit.assert_called_with('python')
        mock_subreddit.top.assert_called_with(limit=3, time_filter='all')

if __name__ == '__main__':
    unittest.main()
