"""File for wrangle data"""
import ast
from typing import Dict, Union, List, Tuple
import re
import pandas as pd
from src.common.utils.config import FunctionRunner

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


class DataWrangler:
    """Class DataWrangler for wrangling data"""
    def __init__(self, data: pd.DataFrame):
        self.data = data

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
                    ):
        """
        Remove outliers from the DataFrame.
        """
        low, high = self.data[column].quantile([lower_quantile, upper_quantile])
        mask_outliers = self.data[column].between(low, high)
        self.data = self.data[mask_outliers]

    def remove_outliers_columns(self, columns: List[str],
                                lower_quantile: float=0.05,
                                upper_quantile: float=0.95
                            ):
        """
        Remove outliers from columns the DataFrame.
        """
        # for column in columns:
        #     low, high = self.data[column].quantile([lower_quantile, upper_quantile])
        #     mask_outliers = self.data[column].between(low, high)
        #     self.data = self.data[mask_outliers]
        for column in columns:
            self.remove_outliers(column, lower_quantile, upper_quantile)


    def drop_null_columns(self, threshold):
        """
        Drop all columns in a pandas DataFrame with greater than a specified percentage
        of null values.

        Parameters:
        data (pandas DataFrame): The input DataFrame
        threshold (float): The maximum percentage of null values allowed (between 0 and 1)

        Returns:
        pandas DataFrame: The DataFrame with null columns dropped
        """
        # Calculate the percentage of null values in each column
        null_percentages = self.data.isnull().sum() / len(self.data)

        # Get the names of columns with null percentages greater than the threshold
        columns_to_drop = null_percentages[null_percentages >= threshold].index

        # Drop the columns from the DataFrame
        self.data = self.data.drop(columns=columns_to_drop)


    def correct_data_types(self,
                           column_data_types: Dict[str, Union[type, Union[Tuple[str, str], List[str]]]
                        ]) -> pd.DataFrame:
        """
        Correct the data types of columns in a pandas DataFrame.

        Parameters:
        data (pandas DataFrame): The input DataFrame
        column_data_types (dictionary): A dictionary of column names and their desired data types, where the value can be either
                                        - a Python type (e.g. int, float, str)
                                        - a tuple with the first element as 'datetime' and the second element as the unit of time (e.g. 's', 'ms')

        Returns:
        pandas DataFrame: The DataFrame with corrected data types

        Example:
        # Define the desired data types for each column
        column_data_types = {'column1': int, 'column2': float,
                            'column3': str, 'column4': ('datetime', 's')}

        # Correct the data types of the columns
        data.correct_data_types(column_data_types)
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
        Drop the specified columns from a pandas DataFrame.

        Parameters:
        df (pandas.DataFrame): The DataFrame to drop columns from.
        columns (list): The list of column names we will drop.

        Returns:
        pandas.DataFrame: A new DataFrame with the specified columns dropped.
        """
        self.data = self.data.drop(columns=columns, errors='ignore')

    def split_column(self, column: str,
                    new_column_name: str,
                    separator: str,
                    index: int
                ) -> None:
        """
        Split a column in the DataFrame into multiple columns based on a delimiter.

        Parameters:
        column (str): The name of the column to split
        new_column_name (str): The name of the new column
        separator (str): The delimiter to split on
        index (int): The index of the new column to extract after splitting

        Returns:
        None. Modifies the input DataFrame in place.
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

        Parameters:
        column (str): The name of the column to clean.
        pattern (str or Pattern): The regular expression pattern to search for.
        new_string (str): The string to replace the matched pattern.

        Raises:
        ValueError: If the column is not found in the DataFrame.
        TypeError: If the column is not a string dtype.

        Returns:
        None. Modifies the input DataFrame in place.
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

    def apply_func_to_df(self, func_name: str, column: str, new_column_name: str, **params):
        """
        Applies a function to each column of a DataFrame.

        Parameters:
        df (pd.DataFrame): The DataFrame to apply the function to.
        func (function): The function to apply to each column.

        Returns:
        A new DataFrame with the function applied to each column.
        """
        func = eval(func_name) # FunctionRunner.run_function(func_name)
        self.data[new_column_name] = self.data[column].apply(func, args=params.values())
