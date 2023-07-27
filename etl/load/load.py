""" ETL- Load Data
"""

from typing import List
from dataclasses import dataclass, field
from src.common.utils.config import Config
from src.common.utils.s3 import S3BucketConnector
from src.common.utils.db_connector import RedshiftConnector
from src.common.utils.logger import logging

# def check_file_added_to_s3(counter_limit: int, sleep_time: int,
#                            prefix: str, s3_bucket: S3BucketConnector
#                            ) -> bool:
#     """checks if a file is added this week or not

#     Args:
#         counter_limit (int): _description_
#         sleep_time (int): _description_
#         prefix (str): _description_
#         s3_bucket (S3BucketConnector): _description_
#     """
#     counter = 0
#     is_added = s3_bucket.check_time_file(prefix)
#     while not is_added:
#         is_added = s3_bucket.check_time_file(prefix)
#         logging.info(f"Checks if a new file is added this week: {is_added}")
#         counter += 1
#         time.sleep(sleep_time)

#         if counter > counter_limit:
#             logging.info(f"No file is added this time for the prefix: {prefix}")
#             return False

@dataclass
class ParamsLoad:
    """Represent paramters for loading data into Redshift"""
    droped_columns_cmt: List[str] = field(default_factory=list)
    droped_columns_pst: List[str] = field(default_factory=list)
    merged_columns_cmt: List[str] = field(default_factory=list)
    merged_columns_pst: List[str] = field(default_factory=list)
    merge_key: str=''


if __name__ == '__main__':
    # activate .env
    Config.load_config()
    # load config
    config = Config.get_config_yml()
    logging.info("load process is starting")
    # params for loading data
    params = ParamsLoad(**config['params_load'])
    # get extracted data from s3 bucket
    logging.info("staring load data into redshift is started")
    s3_bucket_src = S3BucketConnector(bucket=config['s3']['src_bucket'])

    logging.info("get transormed data from s3 buecket")
    # get Posts dataset
    prefix_pst_file = f"{config['folder_bucket']['transformed_posts']}/trasf_pst_"
    IS_FILE_IN_S3 = s3_bucket_src.check_time_file(prefix_pst_file)
     # get the new file in s3 bucket
    file_key = s3_bucket_src.get_last_s3_object_key(prefix_pst_file)

    df_pst = s3_bucket_src.read_s3_csv_to_df(file_key)
    if IS_FILE_IN_S3 and not df_pst.empty:
        # get Comments dataset
        prefix_cmt_file = f"{config['folder_bucket']['transformed_comments']}/trasf_cmt_"
        IS_FILE_IN_S3 = s3_bucket_src.get_last_s3_object_key(prefix_cmt_file)
        file_key = s3_bucket_src.get_last_s3_object_key(prefix_cmt_file)
        df_cmt = s3_bucket_src.read_s3_csv_to_df(file_key)
        if IS_FILE_IN_S3 and not df_pst.empty:
            # pushing data to redshift
            fact_data = df_cmt[params.merged_columns_cmt]\
                            .merge(
                                df_pst[params.merged_columns_pst],
                                on=params.merge_key
                        )
            df_cmt.drop(columns = params.droped_columns_cmt, inplace=True)
            df_pst.drop(columns = params.droped_columns_pst, inplace=True)
            # load data to redshift
            redshift = RedshiftConnector('redshift')
            tables = config['redshift_tables']
            redshift.load_data_from_df(df_pst, tables['posts'])
            redshift.load_data_from_df(df_cmt, tables['comments'])
            redshift.load_data_from_df(fact_data, tables['fact'])

            redshift.disconnect()
            logging.info("load process is finished")
    else:
        logging.info("we don't have any data to push on this time")

# # storing transformed data into a staging area mysql DataBase
# staging = MySQLConnector("mysql")
# logging.info("push transormed data into mysql")
# # save comments
# staging.insert_dataframe(config['mysql_tables']['posts'], df_pst)
# staging.insert_dataframe(config['mysql_tables']['comments'], df_cmt)
# staging.close()

# # data posts
# columns_posts = [
#     'post_id', 'title', 'author', 'subreddit', 'url', 'over_18', 'name','author_premium', 'flair'
# ]

# df_time_pst = DataProcessor.extract_datetime_components(df_pst['created_utc'])
# df_time_pst['time_post_id'] = [str(uuid.uuid4()) for i in range(len(df_time_pst))]
# df_pst_dw = df_pst[columns_posts]
# df_pst_dw['time_post_id'] = df_time_pst['time_post_id']
# # data comments
# columns_comments = ['comment_id', 'body', 'author']
# # 1-data time (Time_Comments)
# df_time = DataProcessor.extract_datetime_components(df_cmt['created_utc'])
# df_time['time_comment_id'] = [str(uuid.uuid4()) for i in range(len(df_time))]
# # load data to table time
# df_cmt_dw = df_cmt[columns_comments]
# df_cmt_dw['time_comment_id'] = df_time['time_comment_id']

# psql = RedshiftConnector("postgresql")
# psql.load_data_from_df(df_time_pst, 'Time_Posts'.lower())
# psql.load_data_from_df(df_pst_dw, 'Posts'.lower())

# psql.load_data_from_df(df_time, 'Time_Comments'.lower())
# psql.load_data_from_df(df_cmt_dw, 'Comments'.lower())
# psql.disconnect()
