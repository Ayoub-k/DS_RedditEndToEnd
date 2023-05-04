"""Transform data
"""
from typing import Tuple, List, Union, Dict
import dataclasses
from src.common.utils.datawrangler import DataWrangler
from src.common.utils.config import Config, TimeFormatter
from src.common.utils.s3 import S3BucketConnector
from src.common.constants.constants import DateFormat, FileType


@dataclasses.dataclass
class ParamsWrangleDate:
    """represents collection of params for data Comments"""
    droped_columns: List[str]
    correct_data_types: Dict[str, Union[type, List[str], Tuple[str, str]]]
    clean_string: Dict[str, str]
    threshold: float
    apply_func_to_df: Dict[str, str]

config = Config.get_config_yml()

# get extracted data from s3 bucket
s3_bucket_src = S3BucketConnector(bucket=config['s3']['src_bucket'])
# get Comments dataset
prefix_cmt_file = f"{config['folder_bucket']['comment_folder']}/cmt_"
file_key = s3_bucket_src.get_last_s3_object_key(prefix_cmt_file)
df_cmt = s3_bucket_src.read_s3_csv_to_df(file_key)
# get Comments dataset
prefix_pst_file = f"{config['folder_bucket']['post_folder']}/pst_"
file_key = s3_bucket_src.get_last_s3_object_key(prefix_pst_file)
df_pst = s3_bucket_src.read_s3_csv_to_df(file_key)

# wrangling Comments data
config_cmt = ParamsWrangleDate(**config["params_comments_data"])
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

# wrangle data posts
config_pst = ParamsWrangleDate(**config["params_posts_data"])
df_wrgle_pst = DataWrangler(data=df_pst)
df_wrgle_pst.correct_data_types(config_pst.correct_data_types)
df_wrgle_cmt.remove_duplicates()
df_wrgle_cmt.drop_null_columns(config_pst.threshold)

df_wrgle_pst.apply_func_to_df(
    **config_pst.apply_func_to_df,
    **{"index": 0, "key": 't'}
)

df_wrgle_cmt.drop_columns(config_pst.droped_columns)

DATE_NOW = TimeFormatter.format_dttime_now(
                DateFormat.DATE_FILE_FORMAT.value
            )
NAME_FILE = f"{DATE_NOW}{FileType.CSV.value}"
# save dataframe df_comments to s3 bucket in comments folder
cmt_file = f"{config['folder_bucket']['transformed_comments']}/trasf_cmt_{NAME_FILE}"
s3_bucket_src.save_df_to_s3(df_wrgle_cmt.data, cmt_file)

pst_file = f"{config['folder_bucket']['transformed_posts']}/trasf_pst_{NAME_FILE}"
s3_bucket_src.save_df_to_s3(df_wrgle_pst.data, pst_file)
