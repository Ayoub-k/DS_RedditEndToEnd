"""File for wrangle data"""
import ast
from typing import Dict, Union, List, Tuple
import re
import pandas as pd
# from src.common.utils.config import FunctionRunner

class DataProcessor:
    """Class DataProcessor for processing data"""

    @staticmethod
    def get_value(value: Union[str, None], index: int, key: str) -> Union[str, None]:
        """
        This function retrieves a specific value from a list of dictionaries,
        given an index and a key.

        Args:
            value (str or None): A string representation of a list of dictionaries.
            index (int): The index of the dictionary in the list.
            key (str): The key whose value we want to retrieve.

        Returns:
            str or None: The value of the specified key in the dictionary at
            the specified index in the list.
        example:
            DataProcessor.get_value('[{"name": "John", "age": 30}, {"name": "Mary", "age": 25}]', 0, 'name')
            Output: "John"
        """
        # Use the `ast.literal_eval` function to safely evaluate the string into a Python object
        my_list = ast.literal_eval(value) if value else []

        # Check if the evaluated value is a list and is not empty
        if isinstance(my_list, list) and my_list:

            # Check if the specified index is within the bounds of the list
            if 0 <= index < len(my_list):

                # Get the dictionary at the specified index and the value of the specified key
                first_dict = my_list[index]
                t_value = first_dict.get(key)

                return t_value

        # Return None if any of the conditions above are not satisfied
        return None

    @staticmethod
    def extract_datetime_components(df_column: pd.Series) -> pd.DataFrame:
        """
        Extracts datetime components from a Series column.

        Args:
            df_column (pandas.Series): The Series containing the datetime column.

        Returns:
            pandas.DataFrame: A DataFrame with columns representing different datetime components
                            (e.g., year, month, day, hour, minute, second).

        Example:
            df = pd.DataFrame({'datetime_column': pd.to_datetime(['2022-01-01 12:30:45', '2022-01-02 09:15:30'])})
            extracted_df = extract_datetime_components(df['datetime_column'])
            print(extracted_df)

            Output:
                year  month  day  hour  minute  second
            0  2022      1    1    12      30      45
            1  2022      1    2     9      15      30
        """
        df_tme = pd.DataFrame()
        date_time = pd.to_datetime(df_column)
        df_tme['year'] = date_time.dt.year
        df_tme['month'] = date_time.dt.month
        df_tme['day'] = date_time.dt.day
        df_tme['hour'] = date_time.dt.hour
        df_tme['minute'] = date_time.dt.minute
        df_tme['second'] = date_time.dt.second
        return df_tme

class DataWrangler:
    """
    Class DataWrangler for wrangling data.

    Args:
        data (pd.DataFrame): The input DataFrame.

    Raises:
        ValueError: If the DataFrame is empty.

    Example:
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        wrangler = DataWrangler(df)
    """
    def __init__(self, data: pd.DataFrame):
        if data.empty:
            raise ValueError("DataFrame is empty")
        self.data = data

    def rename_columns(self, column_mapping: Dict[str, str]) -> None:
        """
        Rename columns in data frame

        Args:
            column_mapping (Dict[str, str]): names of new columns {'old_name': 'new_name'}
        """
        self.data = self.data.rename(columns=column_mapping)

    def remove_duplicates(self):
        """
        Remove duplicate rows from the DataFrame.
        """
        self.data = self.data.drop_duplicates()

    def impute_missing_values(self, impute_value):
        """
        Impute missing values in the DataFrame with a specified value.
        """
        self.data = self.data.fillna(impute_value)


    def remove_outliers(self,
                        column: List[str],
                        lower_quantile: float=0.05,
                        upper_quantile: float=0.95
                    ) -> None:
        """
        Remove outliers from the DataFrame.

        Args:
            column (List[str]): The column(s) from which outliers will be removed.
            lower_quantile (float): The lower quantile threshold for outlier removal. Defaults to 0.05.
            upper_quantile (float): The upper quantile threshold for outlier removal. Defaults to 0.95.

        Returns:
            None: The function modifies the DataFrame in-place.

        Example:
            dw = DataWrangler(dataframe)
            dw.remove_outliers(['age'], lower_quantile=0.1, upper_quantile=0.9)
        """
        low, high = self.data[column].quantile([lower_quantile, upper_quantile])
        mask_outliers = self.data[column].between(low, high)
        self.data = self.data[mask_outliers]

    def remove_outliers_columns(self, columns: List[str],
                                lower_quantile: float=0.05,
                                upper_quantile: float=0.95
                            ) -> None:
        """
        Remove outliers from specified columns of the DataFrame.

        Args:
            columns (List[str]): The columns from which outliers will be removed.
            lower_quantile (float): The lower quantile threshold for outlier removal. Defaults to 0.05.
            upper_quantile (float): The upper quantile threshold for outlier removal. Defaults to 0.95.

        Returns:
            None: The function modifies the DataFrame in-place.

        Example:
            dw = DataWrangler(dataframe)
            dw.remove_outliers_columns(['age', 'income'], lower_quantile=0.1, upper_quantile=0.9)
        """
        for column in columns:
            self.remove_outliers(column, lower_quantile, upper_quantile)


    def drop_null_columns(self, threshold: float=0.5) -> None:
        """
        Drop all columns in the DataFrame with a greater percentage of null values than the specified threshold.

        Args:
            threshold (float): The maximum percentage of null values allowed (between 0 and 1). Defaults to 0.5

        Returns:
            None: The function modifies the DataFrame in-place.

        Example:
            dp = DataWrangler(dataframe)
            dp.drop_null_columns(0.5)
        """
        # Calculate the percentage of null values in each column
        null_percentages = self.data.isnull().sum() / len(self.data)

        # Get the names of columns with null percentages greater than the threshold
        columns_to_drop = null_percentages[null_percentages >= threshold].index

        # Drop the columns from the DataFrame
        self.data = self.data.drop(columns=columns_to_drop)


    def correct_data_types(self,
                           column_data_types: Dict[str, Union[type, Union[Tuple[str, str], List[str]]]
                        ]) -> None:
        """
        Correct the data types of columns in the DataFrame.

        Args:
            column_data_types (Dict[str, Union[type, Union[Tuple[str, str], List[str]]]]):
                A dictionary of column names and their desired data types.
            The value can be:
            - A Python type (e.g., int, float, str)
            - A tuple with the first element as 'datetime' and the second element as the unit of time (e.g., 's', 'ms')
            - A list of strings representing categorical values

        Returns:
            None: The function modifies the DataFrame in-place.

        Example:
            dw = DataWrangler(dataframe)
            column_data_types = {'column1': int, 'column2': float,
                                'column3': str, 'column4': ('datetime', 's')}
            dw.correct_data_types(column_data_types)
        """
        for column, data_type in column_data_types.items():
            if column not in self.data.columns:
                continue

            if isinstance(data_type, (tuple , list)) and data_type[0] == 'datetime':
                self.data[column] = pd.to_datetime(self.data[column], unit=data_type[1])
            else:
                self.data[column] = self.data[column].astype(data_type)


    def drop_columns(self, columns: List[str]):
        """
        Drop the specified columns from the DataFrame.

        Args:
            columns (List[str]): The list of column names to drop.

        Returns:
            None: The function modifies the DataFrame in-place.

        Example:
            dw = DataWrangler(dataframe)
            columns_to_drop = ['column1', 'column2']
            dw.drop_columns(columns_to_drop)
        """
        self.data = self.data.drop(columns=columns, errors='ignore')

    def split_column(self, column: str,
                    new_column_name: str,
                    separator: str,
                    index: int
                ) -> None:
        """
        Split a column in the DataFrame into multiple columns based on a delimiter.

        Args:
            column (str): The name of the column to split.
            new_column_name (str): The name of the new column to create.
            separator (str): The delimiter to split on.
            index (int): The index of the new column to extract after splitting.

        Returns:
            None: Modifies the input DataFrame in place.

        Example:
            dw = DataWrangler(dataframe)
            column_to_split = 'column1'
            new_column = 'new_column'
            sep = ','
            idx = 0
            dw.split_column(column_to_split, new_column, sep, idx)
        """
        if column not in self.data.columns:
            raise ValueError(f"Column '{column}' not found in the DataFrame")

        split_data = self.data[column].str.split(separator, expand=True)
        if index >= split_data.shape[1]:
            raise ValueError(f"Index '{index}' out of range after splitting the column '{column}'")

        self.data[new_column_name] = split_data.iloc[:, index]

    def clean_string(self, column: str, pattern: Union[str, re.Pattern], new_string: str) -> None:
        """
        Clean a string column in the DataFrame from special characters using regex.

        Args:
            column (str): The name of the column to clean.
            pattern (str or re.Pattern): The regular expression pattern to search for.
            new_string (str): The string to replace the matched pattern.

        Raises:
            ValueError: If the column is not found in the DataFrame.
            TypeError: If the column is not a string dtype.

        Returns:
            None: Modifies the input DataFrame in place.

        Example:
            dw = DataWrangler()
            column_to_clean = 'column1'
            pattern_to_replace = r'[^\w\s]'
            replacement_string = ''
            dw.clean_string(column_to_clean, pattern_to_replace, replacement_string)
        """
        if column not in self.data.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        # issubclass(self.data[column].dtype.type, (pd.StringDtype, str))
        if column not in self.data.select_dtypes('object'):
            raise TypeError(f"Column '{column}' is not a string dtype")
        if isinstance(pattern, str):
            try:
                pattern = re.compile(pattern)
            except re.error as error:
                raise ValueError("Invalid regular expression pattern") from error
        self.data[column] = (
            self
            .data[column].str
            .replace(pattern, new_string, regex=True)
            .str.strip()
        )

    def apply_func_to_df(self, func_name: str, column: str, new_column_name: str, **params) -> None:
        """
        Applies a function to each column of a DataFrame.

        Args:
            func_name (str): The name of the function to apply.
            column (str): The name of the column to apply the function to.
            new_column_name (str): The name of the new column to store the results.
            **params: Additional parameters to be passed to the function.

        Returns:
            pd.DataFrame: A new DataFrame with the function applied to each column.

        Example:
            dw = DataWrangler(dataframe)
            function_name = 'my_function'
            column_to_apply = 'column1'
            new_column = 'result'
            dw.apply_func_to_df(function_name, column_to_apply, new_column, param1=1, param2='abc')
        """
        func = eval(func_name) # FunctionRunner.run_function(func_name)
        self.data[new_column_name] = self.data[column].apply(func, args=params.values())
