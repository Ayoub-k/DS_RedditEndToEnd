""" Connector and methods accessing to S3
"""
import os
from io import StringIO, BytesIO
from typing import List, Optional
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from src.common.constants.constants import FileType
from src.common.utils.logger import logging
from src.common.utils.config import Config


class S3BucketConnector():

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

    def get_last_s3object_key(self, prefix: str) -> Optional[str]:
        """
        Returns the key of the last object in the S3
        bucket with the specified prefix.

        Parameters:
            prefix (str): The prefix to filter objects in the S3 bucket.

        Returns:
            The key of the last object with the specified prefix, or None
            if no matching objects are found.
        """
        try:
            objects = list(self._bucket.objects.filter(Prefix=prefix))
            if not objects:
                return None
            last_object = max(objects, key=lambda obj: obj.last_modified)
            return last_object.key
        except ClientError as error:
            raise ValueError(f"Failed to get last S3 object key for prefix '{prefix}'.") from error

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
