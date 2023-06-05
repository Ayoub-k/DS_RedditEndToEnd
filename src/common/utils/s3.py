""" Connector and methods accessing to S3
"""
import os
from datetime import datetime, timedelta
from io import StringIO, BytesIO
from typing import List, Optional
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from src.common.constants.constants import FileType
from src.common.utils.logger import logging
from src.common.utils.config import Config


class S3BucketConnector:

    """Class for interacting with S3 Bucket
    """

    def __init__(self, bucket: str):
        """Constructor for S3BucketConnector

        Args:
            bucket (str): bucket name
        """
        config = Config.get_config_yml(section_key='s3')
        self.session = boto3.Session(
            aws_access_key_id=os.getenv(config['access_key']),
            aws_secret_access_key=os.getenv(config['secret_key'])
        )
        self._s3 = self.session.resource(
            service_name = 's3'
        )
        self._bucket = self._s3.Bucket(bucket)

    def get_keys_in_prefix(self, prefix: str) -> List[str]:
        """List of file in prefix

        Args:
            prefix (str): prefix for filter data in s3 bucket

        Returns:
            List[str]: list of keys to get data from s3 bucket by key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        logging.info("getting keys from s3 bucket using prefix")
        return files

    def read_s3_csv_to_df(self,
                          key_object: str,
                          encoding: str="utf-8",
                          sep: str=','
                          ) -> pd.DataFrame:
        """Read csv file from s3 bucket into a dataframe

        Args:
            key_object (str): key object in s3 bucket
            encoding (str, optional): type encoding. Defaults to "utf-8".
            sep (str, optional): sep to read csv file into df. Defaults to ','.

        Returns:
            DataFrame: return dataframe
        """
        csv_obj = self._bucket.Object(key=key_object).get().get("Body").read().decode(encoding)
        data = StringIO(csv_obj)
        dataframe = pd.read_csv(data, delimiter = sep)
        logging.info("read csv into datafarme from s3 bucket")
        return dataframe

    def save_df_to_s3(self, dataframe: pd.DataFrame, key_object: str):
        """write dataframe to s3 bucket

        Args:
            dataframe (DataFrame): dataframe we want to save in s3 bucket
            key_object (str): name object (dataframe) in s3 bucket
        """
        out_buffer = None
        _, file_extension = os.path.splitext(key_object)
        if file_extension == FileType.CSV.value:
            out_buffer = StringIO()
            dataframe.to_csv(out_buffer, index=False)
        if file_extension == FileType.PARQUET.value:
            out_buffer = BytesIO()
            dataframe.to_parquet(out_buffer, index=False)
        if out_buffer is not None:
            self._bucket.put_object(Body=out_buffer.getvalue(), Key=key_object)
            logging.info("dataframe is wrote in s3 bucket")
        else:
            logging.error("Failed to write DataFrame to S3: no valid file extension found.")
            raise ValueError("Failed to write DataFrame to S3: no valid file extension found.")

    def get_lastobject_s3bucket(self, prefix: str):
        """
        Returns the key of the most recently modified S3 object with the given prefix.

        Args:
            prefix (str): The prefix to search for.

        Returns:
            str: The key of the most recently modified object, or None
            if no matching objects were found.

        Raises:
            ValueError: If the specified prefix is invalid.
        """

        paginator = self._s3.meta.client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=self._bucket.name, Prefix=prefix)

        last_modified = None
        last_object = None
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].startswith(prefix):
                        if last_modified is None or obj['LastModified'] > last_modified:
                            last_modified = obj['LastModified']
                            last_object = obj

        if last_object is None:
            logging.warning(f"No matching objects found with prefix '{prefix}'")
        return last_object

    def get_last_s3_object_key(self, prefix: str) -> str:
        """
        Returns the key of the most recently modified S3 object with the given prefix.

        Args:
            prefix (str): The prefix to search for.

        Returns:
            str: The key of the most recently modified object, or None
            if no matching objects were found.

        Raises:
            ValueError: If the specified prefix is invalid.
        """

        paginator = self._s3.meta.client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=self._bucket.name, Prefix=prefix)

        last_modified = None
        last_object_key = None
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].startswith(prefix):
                        if last_modified is None or obj['LastModified'] > last_modified:
                            last_modified = obj['LastModified']
                            last_object_key = obj['Key']

        if last_object_key is None:
            logging.warning(f"No matching objects found with prefix '{prefix}'")
        return last_object_key

    def check_file_added_to_s3_this_week(self, prefix: str) -> bool:
        """
        Check if the last file added to the S3 bucket with the given prefix is from the current week

        Args:
            prefix (str): The prefix of the file in the S3 bucket.

        Returns:
            bool: True if the last file added is from the current week, False otherwise.
        """
        file_obj = self.get_lastobject_s3bucket(prefix)
        if file_obj is None:
            return False

        current_week_start = datetime.now().date() - timedelta(days=datetime.now().date().weekday())
        current_week_end = current_week_start + timedelta(days=6)

        last_modified_date = file_obj['LastModified'].date()
        return current_week_start <= last_modified_date <= current_week_end


class S3BucketConnectorV2:
    """Class for interacting with an S3 Bucket."""

    def __init__(self, bucket: str):
        """Constructor for S3BucketConnector.

        Args:
            bucket (str): The bucket name.
        """
        config = Config.get_config_yml(section_key='s3')
        self.session = boto3.Session(
            aws_access_key_id=os.getenv(config['access_key']),
            aws_secret_access_key=os.getenv(config['secret_key'])
        )
        self.s3_client = self.session.client('s3')
        self.bucket_name = bucket

    def get_keys_in_prefix(self, prefix: str) -> List[str]:
        """Get a list of keys in the specified prefix of the S3 bucket.

        Args:
            prefix (str): The prefix for filtering data in the S3 bucket.

        Returns:
            List[str]: A list of keys to access data from the S3 bucket.
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            files = [obj['Key'] for obj in response['Contents']]
            logging.info(f"Retrieved keys from S3 bucket using prefix: {prefix}")
            return files
        except ClientError as error:
            logging.error(f"Failed to get keys from S3 bucket using prefix: {prefix} Error {error}")
            return []

    def read_s3_csv_to_df(self, key_object: str, encoding: str = "utf-8", sep: str = ','
                        ) -> pd.DataFrame:
        """Read a CSV file from the S3 bucket into a DataFrame.

        Args:
            key_object (str): The key object in the S3 bucket.
            encoding (str, optional): The encoding type. Defaults to "utf-8".
            sep (str, optional): The separator to read the CSV file into a DataFrame Defaults to ','

        Returns:
            DataFrame: The resulting DataFrame.
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key_object)
            csv_obj = response['Body'].read().decode(encoding)
            data = StringIO(csv_obj)
            dataframe = pd.read_csv(data, delimiter=sep)
            logging.info(f"Read CSV into DataFrame from S3 bucket. Key: {key_object}")
            return dataframe
        except ClientError as error:
            logging.error(f"Failed to read CSV from S3 bucket. Key: {key_object}. Error: {error}")
            return pd.DataFrame()

    def save_df_to_s3(self, dataframe: pd.DataFrame, key_object: str):
        """Write a DataFrame to the S3 bucket.

        Args:
            dataframe (DataFrame): The DataFrame to save in the S3 bucket.
            key_object (str): The name of the object (DataFrame) in the S3 bucket.
        """
        _, file_extension = os.path.splitext(key_object)
        out_buffer = None

        if file_extension == FileType.CSV.value:
            out_buffer = StringIO()
            dataframe.to_csv(out_buffer, index=False)
        elif file_extension == FileType.PARQUET.value:
            out_buffer = BytesIO()
            dataframe.to_parquet(out_buffer, index=False)

        if out_buffer is not None:
            try:
                self.s3_client.put_object(
                    Body=out_buffer.getvalue(),
                    Bucket=self.bucket_name, Key=key_object
                    )
                logging.info(f"DataFrame is written to S3 bucket. Key: {key_object}")
            except ClientError as error:
                logging.error(
                    f"Failed to write DataFrame to S3 bucket. Key: {key_object}. Error: {error}"
                )
        else:
            logging.error("Failed to write DataFrame to S3: no valid file extension found.")
            raise ValueError("Failed to write DataFrame to S3: no valid file extension found.")

    def get_lastobject_s3bucket(self, prefix: str):
        """Get the most recently modified S3 object key with the given prefix.

        Args:
            prefix (str): The prefix to search for.

        Returns:
            dict: The metadata of the most recently modified object,
                  or None if no matching objects were found.
        """
        paginator = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        page_iterator = paginator.paginate(Bucket=self.bucket_name.name, Prefix=prefix)

        last_modified = None
        last_object = None
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].startswith(prefix):
                        if last_modified is None or obj['LastModified'] > last_modified:
                            last_modified = obj['LastModified']
                            last_object = obj

        if last_object is None:
            logging.warning(f"No matching objects found with prefix '{prefix}'")
        return last_object

    def check_file_added_to_s3_this_week(self, prefix: str) -> bool:
        """
        Check if the last file added to the S3 bucket with the given prefix is from the current week

        Args:
            prefix (str): The prefix of the file in the S3 bucket.

        Returns:
            bool: True if the last file added is from the current week, False otherwise.
        """
        last_object = self.get_lastobject_s3bucket(prefix)
        if last_object is None:
            return False
        logging.info(last_object)
        current_week_start = datetime.now().date() - timedelta(days=datetime.now().date().weekday())
        current_week_end = current_week_start + timedelta(days=6)

        last_modified_date = last_object['LastModified'].date()
        return current_week_start <= last_modified_date <= current_week_end

    def get_last_s3_object_key(self, prefix: str) -> str:
        """
        Returns the key of the most recently modified S3 object with the given prefix.

        Args:
            prefix (str): The prefix to search for.

        Returns:
            str: The key of the most recently modified object, or None
            if no matching objects were found.

        Raises:
            ValueError: If the specified prefix is invalid.
        """

        paginator = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        page_iterator = paginator.paginate(Bucket=self.bucket_name.name, Prefix=prefix)


        last_modified = None
        last_object_key = None
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].startswith(prefix):
                        if last_modified is None or obj['LastModified'] > last_modified:
                            last_modified = obj['LastModified']
                            last_object_key = obj['Key']

        if last_object_key is None:
            logging.warning(f"No matching objects found with prefix '{prefix}'")
        return last_object_key
