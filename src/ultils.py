import re
import pytz
from datetime import datetime, timezone


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
