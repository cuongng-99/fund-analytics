import re
import pytz
from datetime import datetime, timezone
import pandas as pd
import numpy as np


def unix_to_datetime_with_timezone(unix_timestamp):
    tz = pytz.timezone("Asia/Ho_Chi_Minh")
    if unix_timestamp:
        # Convert the Unix timestamp to a datetime object in UTC
        utc_dt = datetime.fromtimestamp(unix_timestamp / 1000, tz=timezone.utc)
        # Convert the datetime object to the specified timezone
        dt_with_timezone = utc_dt.astimezone(tz)
        return dt_with_timezone


def camel_to_snake(camel_str):
    # Insert underscores only before uppercase letters that are followed by a lowercase letter or number
    snake_case = re.sub(r"(?<!^)(?<!_)(?=[A-Z][a-z0-9])", "_", camel_str)
    snake_case = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", snake_case).lower()
    return snake_case


def convert_numeric_nan_to_none(df) -> pd.DataFrame:
    """
    This function checks all columns in the input DataFrame. If a column is of any numeric type
    and contains NaN values, it converts the column to object type and replaces NaN values with None.

    Parameters:
        df (pd.DataFrame): Input DataFrame

    Returns:
        pd.DataFrame: Modified DataFrame with NaN replaced by None in relevant columns
    """
    # Iterate through each column and check for numeric columns with NaN values
    for column in df.columns:
        # Check if the column is of numeric type and contains NaN
        if pd.api.types.is_numeric_dtype(df[column]) and df[column].isnull().any():
            # Convert the column to object type
            df[column] = df[column].astype(object)

    # Replace NaN with None in the entire DataFrame
    df = df.where(pd.notna(df), None)

    return df
