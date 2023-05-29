"""Transform data
"""
import sys
import time
from typing import Tuple, List, Union, Dict
from dataclasses import dataclass, field
from src.common.utils.datawrangler import DataWrangler
from src.common.utils.config import Config, TimeFormatter
from src.common.utils.s3 import S3BucketConnector, S3BucketConnectorV2
from src.common.constants.constants import DateFormat, FileType
from src.common.utils.logger import logging


@dataclass
class ParamsWrangleData:
    """Represents collection of params for data Comments"""
    droped_columns: List[str] = field(default_factory=list)
    correct_data_types: Dict[str, Union[type, List[str], Tuple[str, str]]] = field(default_factory=dict)
    clean_string: Dict[str, str] = field(default_factory=dict)
    threshold: float = 0.5
    apply_func_to_df: Dict[str, str] = field(default_factory=dict)
    rename_columns: Dict[str, str] = field(default_factory=dict)

def check_file_added_to_s3(counter_limit: int, sleep_time: int,
                           prefix: str, s3_bucket: S3BucketConnectorV2
                           ) -> None:
    """checks if a file is added this week or not

    Args:
        counter_limit (int): _description_
        sleep_time (int): _description_
        prefix (str): _description_
        s3_bucket (S3BucketConnector): _description_
    """
    counter = 0
    is_added = s3_bucket.check_file_added_to_s3_this_week(prefix)
    while not is_added:
        is_added = s3_bucket.check_file_added_to_s3_this_week(prefix)
        logging.info(f"Checks if a new file is added this week: {is_added}")
        counter += 1
        time.sleep(sleep_time)

        if counter > counter_limit:
            logging.info(f"No file is added this week for the prefix: {prefix}")
            sys.exit(1)

if __name__ == '__main__':
    Config.load_config()
    config = Config.get_config_yml()

    # get extracted data from s3 bucket
    s3_bucket_src = S3BucketConnectorV2(bucket=config['s3']['src_bucket'])
    # get Comments dataset
    prefix_cmt_file = f"{config['folder_bucket']['comment_folder']}/cmt_"
    # check is file in s3 bucket is added this week
    
    check_file_added_to_s3(config['config_time_files']['epoch'],
                           config['config_time_files']['time'],
                           prefix_cmt_file,
                           s3_bucket_src
                        )
    file_key = s3_bucket_src.get_last_s3_object_key(prefix_cmt_file)
    df_cmt = s3_bucket_src.read_s3_csv_to_df(file_key)
    # get Comments dataset
    prefix_pst_file = f"{config['folder_bucket']['post_folder']}/pst_"
    # check is file in s3 bucket is added this week
    check_file_added_to_s3(config['config_time_files']['epoch'],
                           config['config_time_files']['time'],
                           prefix_pst_file,
                           s3_bucket_src
                        )
    file_key = s3_bucket_src.get_last_s3_object_key(prefix_pst_file)
    df_pst = s3_bucket_src.read_s3_csv_to_df(file_key)

    # wrangling Comments data
    config_cmt = ParamsWrangleData(**config["params_comments_data"])
    df_wrgle_cmt = DataWrangler(data=df_cmt)
    # correct type
    df_wrgle_cmt.correct_data_types(
        config_cmt.correct_data_types
    )
    # clean column `"body"` from special characters
    df_wrgle_cmt.clean_string(**config_cmt.clean_string)
    # remove duplicated values
    df_wrgle_cmt.remove_duplicates()
    # remove columns have a lot of null values
    df_wrgle_cmt.drop_null_columns(config_cmt.threshold)
    # drop any column exists in droped_columns list
    df_wrgle_cmt.drop_columns(config_cmt.droped_columns)
    # rename columns
    df_wrgle_cmt.rename_columns(config_cmt.rename_columns)

    # wrangle data posts
    config_pst = ParamsWrangleData(**config["params_posts_data"])
    df_wrgle_pst = DataWrangler(data=df_pst)
    df_wrgle_pst.correct_data_types(config_pst.correct_data_types)
    df_wrgle_pst.remove_duplicates()
    df_wrgle_pst.drop_null_columns(config_pst.threshold)

    df_wrgle_pst.apply_func_to_df(
        **config_pst.apply_func_to_df,
        **{"index": 0, "key": 't'}
    )

    df_wrgle_pst.drop_columns(config_pst.droped_columns)

    DATE_NOW = TimeFormatter.format_dttime_now(
                    DateFormat.DATE_FILE_FORMAT.value
                )
    NAME_FILE = f"{DATE_NOW}{FileType.CSV.value}"
    # save dataframe df_comments to s3 bucket in comments folder
    cmt_file = f"{config['folder_bucket']['transformed_comments']}/trasf_cmt_{NAME_FILE}"
    s3_bucket_src.save_df_to_s3(df_wrgle_cmt.data, cmt_file)

    pst_file = f"{config['folder_bucket']['transformed_posts']}/trasf_pst_{NAME_FILE}"
    s3_bucket_src.save_df_to_s3(df_wrgle_pst.data, pst_file)
