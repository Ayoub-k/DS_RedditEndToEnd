"""Testing Utils.py
"""
import unittest
from datetime import datetime, timedelta
from src.common.utils.utils import TimeUtils

class TimeUtilsTestCase(unittest.TestCase):
    """Class for testing Class TimeUtils
    """

    def test_seconds_to_datetime_success(self):
        """For testiog method second_to_datetime"""
        result = TimeUtils.seconds_to_datetime(1000000000)
        result_expected = datetime.fromtimestamp(1000000000)
        self.assertEqual(result, result_expected)
        result = TimeUtils.seconds_to_datetime('1000000000')
        self.assertEqual(result, result_expected)
        result = TimeUtils.seconds_to_datetime(1000000000.5)
        result_expected = datetime.fromtimestamp(1000000000.5)
        self.assertEqual(result, result_expected)

    def test_second_to_datetime_fails(self):
        """Test second_to_datetime case fails
        """
        with self.assertRaises(ValueError):
            TimeUtils.seconds_to_datetime("abcd")
            TimeUtils.seconds_to_datetime(1000000000000000000000)

        with self.assertRaises(TypeError):
            TimeUtils.seconds_to_datetime(["abcd"])
            TimeUtils.seconds_to_datetime(["6515115"])
            TimeUtils.seconds_to_datetime([6515115])

    def test_get_time_interval_success(self):
        """Test function get_time_interval"""
        date_now = datetime.now()
        # test interval hour
        start_time = date_now.replace(minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(hours=1)
        result = TimeUtils.get_time_interval('hour')
        self.assertEqual(result, (start_time, end_time))
        # test inteval day
        start_time = date_now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(days=1)
        result = TimeUtils.get_time_interval('day')
        self.assertEqual(result, (start_time, end_time))
        # test week
        start_time = date_now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=date_now.weekday())
        end_time =start_time + timedelta(days=7)
        result = TimeUtils.get_time_interval('week')
        self.assertEqual(result, (start_time, end_time))
        # test month
        start_time = date_now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start_time.month == 12:
            end_time = start_time.replace(year=start_time.year + 1, month=1)
        else:
            end_time = start_time.replace(month=start_time.month + 1)
        result = TimeUtils.get_time_interval('Month')
        self.assertEqual(result, (start_time, end_time))

    def test_get_time_interval_fails(self):
        """Test function get_time_interval case fails"""
        with self.assertRaises((TypeError, ValueError)):
            TimeUtils.get_time_interval(45)
            TimeUtils.get_time_interval('all')

if __name__ == '__main__':
    unittest.main()
