from datetime import timedelta,datetime
def convert_to_decimal_hours(time_str):
    try:
        print(time_str)
        # Check if the input is a floating point number (like '21.38')
        if '.' in time_str:
            hours = float(time_str)
            return hours  # Directly return the number if it's a decimal

        # If not a floating point, split the string into hours, minutes, and seconds
        hours, minutes, seconds = map(int, time_str.split(":"))

        # Convert minutes to a fraction of an hour
        fraction = minutes / 60.0

        # Return the sum of hours and fraction
        return hours + fraction
    except ValueError:
        # Return 0 if an error occurs during conversion
        return 0


def timedelta_to_str(time_delta):
    if isinstance(time_delta, str):
        return time_delta
    total_seconds = time_delta.total_seconds()
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def str_to_timedelta(time_str):
    if len(time_str.split(":")) == 4:
        # Handle format like '0:05:38:21' (days:hours:minutes:seconds)
        days, hours, minutes, seconds = map(int, time_str.split(":"))
        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    elif "." in time_str:
        # Handle format like '21.38'
        hours, fraction = map(float, time_str.split("."))
        minutes = fraction * 60
        return timedelta(hours=hours, minutes=minutes)
    else:
        # Handle format like '05:38:21' (hours:minutes:seconds)
        days, hours, minutes = map(int, time_str.split(":"))
        return timedelta(days=days, hours=hours, minutes=minutes)


def sanitize_sheet_name(name):
    invalid_chars = [":", "/", "?", "*", "[", "]"]
    for char in invalid_chars:
        name = name.replace(char, "_")  # replace invalid characters with underscore
    return name


def ensure_timedelta(value):
    if isinstance(value, timedelta):
        return value
    elif isinstance(value, str):
        return str_to_timedelta(value)
    else:
        return timedelta(0)


def ensure_int(value):
    if isinstance(value, int):
        return float(value)
    elif isinstance(value, str):
        return float(value)
    elif isinstance(value, float):
        return value
    else:
        return 0

def parse_datetime(data):
    # Splitting the string at the decimal point to ignore fractional seconds
    date_string = data.split('.')[0]
    # Parsing the date-time string without fractional seconds
    return datetime.fromisoformat(date_string)