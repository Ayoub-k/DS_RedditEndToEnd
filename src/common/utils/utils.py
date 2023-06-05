""" Utils
"""
from typing import Tuple, Union
from datetime import datetime, timedelta

class TimeUtils:
    """
    A utility class for time-related operations.

    Methods:
        seconds_to_datetime(seconds: Union[int, str]) -> datetime:
            Convert the given number of seconds to a datetime object.

        get_time_interval(interval: str) -> Tuple[datetime, datetime]:
            Get the start and end datetime for the specified interval.

    Example:
        from datetime import datetime
        from typing import Tuple
        from time import sleep

        class TimeUtils:
            # ... implementation ...

        # Create an instance of TimeUtils
        utils = TimeUtils()

        # Convert seconds to datetime
        dt = utils.seconds_to_datetime(1617269784)
        print(dt)  # Output: 2021-04-01 10:56:24

        # Get the time interval for a week
        start, end = utils.get_time_interval('week')
        print(start, end)  # Output: 2021-05-17 00:00:00 2021-05-24 00:00:00
    """
    @staticmethod
    def seconds_to_datetime(seconds: Union[str, int, float]) -> datetime:
        """
        Convert the given number of seconds to a datetime object.

        Args:
            seconds (int or str): The number of seconds.

        Returns:
            datetime: The datetime object representing the given number of seconds.

        Raises:
            ValueError: If the input is not a valid number of seconds.
        """
        if isinstance(seconds, str):
            try:
                seconds = int(seconds)
            except ValueError as error:
                raise ValueError("Invalid number of seconds") from error
        elif not isinstance(seconds, (int, float)):
            raise TypeError("Seconds must be an integer or float")

        try:
            return datetime.fromtimestamp(seconds)
        except ValueError as error:
            raise ValueError("Invalid number of seconds") from error


    @staticmethod
    def get_time_interval(interval: str) -> Tuple[datetime, datetime]:
        """
        Get the start and end datetime for the specified interval.

        Args:
            interval (str): The interval, such as 'week', 'day', 'hour', or 'month'.

        Returns:
            tuple: A tuple containing the start and end datetime objects.
        """
        now = datetime.now()

        if not isinstance(interval, str):
            raise TypeError("Interval must be a string.")

        interval = interval.lower()  # Convert to lowercase for case-insensitive comparison

        if interval == 'week':
            start_time = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=now.weekday())
            end_time = start_time + timedelta(days=7)
        elif interval == 'day':
            start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(days=1)
        elif interval == 'hour':
            start_time = now.replace(minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(hours=1)
        elif interval == 'month':
            start_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if start_time.month == 12:
                end_time = start_time.replace(year=start_time.year + 1, month=1)
            else:
                end_time = start_time.replace(month=start_time.month + 1)
        else:
            raise ValueError("Supported intervals are 'week', 'day', 'hour', and 'month'.")

        return start_time, end_time
