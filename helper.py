from datetime import datetime, timedelta
import secrets
import string
import pytz
from pymongo import MongoClient, UpdateOne


def generate_custom_key(length=20):
    # Define the character pool
    alphabet = string.ascii_letters + string.digits + "_-"

    # Generate the key
    key = ''.join(secrets.choice(alphabet) for _ in range(length))

    return key


def date_conversion(match_date, date_format):
    """
    Gets date and its format and converts into required format
    Returns timezone-aware datetime object in UTC

    :param match_date: str or datetime - date of match
    :param date_format: str - format of date string
    :return: datetime - timezone-aware match date in UTC
    """
    if isinstance(match_date, datetime):
        # If already a datetime object, ensure it's UTC
        if match_date.tzinfo is None:
            # Naive datetime, assume UTC
            return match_date.replace(tzinfo=pytz.UTC)
        else:
            # Convert to UTC if not already
            return match_date.astimezone(pytz.UTC)
    
    # Parse string to datetime object
    dt_object = datetime.strptime(str(match_date), date_format)
    
    # If the parsed datetime is naive, assume UTC
    if dt_object.tzinfo is None:
        return dt_object.replace(tzinfo=pytz.UTC)
    else:
        # Convert to UTC if timezone-aware
        return dt_object.astimezone(pytz.UTC)


def local_to_utc(local_dt_str, local_tz_str, date_format):
    """
    Convert local datetime string to UTC timezone-aware datetime object
    
    :param local_dt_str: str - local datetime string
    :param local_tz_str: str - timezone string (e.g., 'UTC', 'Europe/Vienna')
    :param date_format: str - format of the datetime string
    :return: datetime - timezone-aware UTC datetime object
    """
    # Parse the local date and time string
    local_dt = datetime.strptime(local_dt_str, date_format)

    # Handle special case for UTC timezone
    if local_tz_str.lower() == 'utc':
        # If already UTC, just add timezone info
        return local_dt.replace(tzinfo=pytz.UTC)
    
    # Get the local timezone
    try:
        local_tz = pytz.timezone(local_tz_str)
    except pytz.exceptions.UnknownTimeZoneError:
        # If timezone is unknown, assume UTC
        print(f"Warning: Unknown timezone '{local_tz_str}', assuming UTC")
        return local_dt.replace(tzinfo=pytz.UTC)

    # Localize the datetime object to the local timezone
    local_dt = local_tz.localize(local_dt, is_dst=None)

    # Convert the localized datetime to UTC
    utc_dt = local_dt.astimezone(pytz.UTC)

    return utc_dt


def parse_tipico_date(date_str):
    """
    Parse Tipico date format specifically and convert to UTC datetime object
    
    :param date_str: str - date string in Tipico format
    :return: datetime - timezone-aware UTC datetime object
    """
    try:
        # Try parsing with timezone info first
        date_formats = [
            '%d %b %Y %H:%M:%S %Z',  # "25 Dec 2024 15:30:00 UTC"
            '%d %b %Y %H:%M:%S',     # "25 Dec 2024 15:30:00"
            '%Y-%m-%d %H:%M:%S',     # "2024-12-25 15:30:00"
            '%Y-%m-%dT%H:%M:%S%z',   # "2024-12-25T15:30:00+00:00"
            '%Y-%m-%dT%H:%M:%SZ',    # "2024-12-25T15:30:00Z"
        ]
        
        for date_format in date_formats:
            try:
                if '%Z' in date_format or '%z' in date_format:
                    # Timezone-aware parsing
                    dt_object = datetime.strptime(date_str, date_format)
                    if dt_object.tzinfo is None:
                        # If parsing didn't add timezone info, assume UTC
                        return dt_object.replace(tzinfo=pytz.UTC)
                    else:
                        # Convert to UTC
                        return dt_object.astimezone(pytz.UTC)
                else:
                    # Naive datetime parsing, assume UTC
                    dt_object = datetime.strptime(date_str, date_format)
                    return dt_object.replace(tzinfo=pytz.UTC)
            except ValueError:
                continue
        
        # If all formats fail, try the helper function approach
        return local_to_utc(date_str, 'utc', '%d %b %Y %H:%M:%S')
        
    except Exception as e:
        print(f"Error parsing date '{date_str}': {e}")
        # Return current UTC time as fallback
        return datetime.now(pytz.UTC)


def is_next_8_day_match(match_date, date_format=None):
    """
    Check if match is within next 8 days
    
    :param match_date: datetime or str - match date
    :param date_format: str - format if match_date is string
    :return: bool - True if within next 8 days
    """
    if isinstance(match_date, str):
        if date_format is None:
            raise ValueError("date_format required when match_date is string")
        # Parse the date string into a datetime object
        given_date = datetime.strptime(match_date, date_format)
        # Ensure timezone-aware
        if given_date.tzinfo is None:
            given_date = given_date.replace(tzinfo=pytz.UTC)
    else:
        # Assume it's already a datetime object
        given_date = match_date
        if given_date.tzinfo is None:
            given_date = given_date.replace(tzinfo=pytz.UTC)

    # Get the current UTC time
    current_date = datetime.now(pytz.UTC)

    # Calculate the difference between the given date and now
    time_difference = given_date - current_date

    # Check if the given date is within the next 8 days
    return time_difference <= timedelta(days=8) and time_difference >= timedelta(0)


def store_data_into_mongodb(matches_data, database_name):
    """
    Store match data into MongoDB with consistent timestamp handling
    """
    client = MongoClient('mongodb://localhost:27017')
    db = client['betting']
    collection = db[database_name]
    
    operations = []
    for match in matches_data:
        # Ensure timestamp is timezone-aware UTC datetime
        if 'timestamp' in match:
            if isinstance(match['timestamp'], str):
                # If timestamp is string, convert to datetime
                match['timestamp'] = parse_tipico_date(match['timestamp'])
            elif isinstance(match['timestamp'], datetime):
                if match['timestamp'].tzinfo is None:
                    # If naive datetime, assume UTC
                    match['timestamp'] = match['timestamp'].replace(tzinfo=pytz.UTC)
                else:
                    # Convert to UTC if not already
                    match['timestamp'] = match['timestamp'].astimezone(pytz.UTC)
        
        filter_query = {
            "match_id": match['match_id'],
            "timestamp": match['timestamp'],
            "competitor1": match['competitor1'],
            "competitor2": match['competitor2']
        }
        operations.append(UpdateOne(filter_query, {'$set': match}, upsert=True))

    if operations:
        collection.bulk_write(operations)


def store_competitor_mapping_data(docs):
    client = MongoClient('mongodb://localhost:27017')
    db = client['betting']
    collection = db['competitor_mapping']
    operations = []

    for doc in docs:
        # Normalize maps to always be a list
        maps_value = doc.get('maps')
        if isinstance(maps_value, str):
            doc['maps'] = [maps_value.lower()]

        operations.append(
            UpdateOne(
                {'id': doc['id'].lower()},
                {"$addToSet": {"maps": {"$each": doc['maps']}}},
                upsert=True
            )
        )

    if operations:
        result = collection.bulk_write(operations)
        print(f"Bulk write completed: {result.bulk_api_result}")


def normalize_timestamp_for_comparison(timestamp):
    """
    Normalize timestamp for MongoDB comparison operations
    Ensures consistent timezone-aware UTC datetime objects
    
    :param timestamp: datetime, str, or other timestamp format
    :return: datetime - timezone-aware UTC datetime object
    """
    if isinstance(timestamp, datetime):
        if timestamp.tzinfo is None:
            # Naive datetime, assume UTC
            return timestamp.replace(tzinfo=pytz.UTC)
        else:
            # Convert to UTC
            return timestamp.astimezone(pytz.UTC)
    elif isinstance(timestamp, str):
        # Try to parse string timestamp
        return parse_tipico_date(timestamp)
    else:
        # Try to convert to datetime
        try:
            dt = datetime.fromtimestamp(float(timestamp), tz=pytz.UTC)
            return dt
        except (ValueError, TypeError):
            raise ValueError(f"Cannot parse timestamp: {timestamp}")


def update_data():
    client = MongoClient('mongodb://localhost:27017')
    db = client['betting']
    collection = db['matches_data']