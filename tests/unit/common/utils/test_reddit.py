"""Testing class ScrapperReddit"""

from typing import List
import unittest
from src.common.utils.reddit import RedditScraper, Post, RedditAuth, RedditSearch
from src.common.utils.config import Config


class RedditScraperTestCase(unittest.TestCase):
    """Test class RedditScraper"""

    def setUp(self) -> None:
        """Set up reddit auth"""
        Config.load_config()
        # load configuration from config.yml
        config = Config.get_config_yml()
        # Create redditScraper
        self.scraper = RedditScraper(RedditAuth(**config['reddit']))

    def test_get_posts_success(self):
        """Testing"""
        # Create a RedditSearch object
        search_filter = RedditSearch(
            subreddit='sports',
            limit=3, time_filter='month')
        # Call the method being tested
        result = self.scraper.get_posts(search_filter)

        # Assertions
        self.assertEqual(len(result), 3)
        for post in result:
            self.assertIsInstance(post, Post)
            self.assertIn('sports', post.title.lower())
        self.assertIsInstance(result[0].score, int)

    def test_get_posts_fails(self):
        """testing fails get_posts"""
        # Create a RedditSearch object
        search_filter = RedditSearch(
            subreddit='sports',
            limit=3, time_filter='d')
        # Call the method being tested
        with self.assertRaises(ValueError):
            self.scraper.get_posts(search_filter)

if __name__ == '__main__':
    unittest.main()
