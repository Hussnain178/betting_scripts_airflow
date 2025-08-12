import logging
import os
from datetime import datetime, timedelta
import secrets
import string
import pytz
from pymongo import MongoClient, UpdateOne
from rapidfuzz import fuzz
import re


def setup_scraper_logger(scraper_name):
    """
    Set up logging configuration for scrapers
    Creates persistent log files that append to same file
    Truncates file when it exceeds 10MB

    :param scraper_name: str - name of the scraper (e.g., 'tipico', 'bovada')
    :return: logger object
    """
    # Create logs directory if it doesn't exist
    log_directory = 'scraper_logs'
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Create persistent log filename (no timestamp)
    log_filename = f"{log_directory}/{scraper_name}.log"

    # Check file size and truncate if over 10MB
    max_size = 10 * 1024 * 1024  # 10MB in bytes
    if os.path.exists(log_filename):
        file_size = os.path.getsize(log_filename)
        if file_size > max_size:
            # Truncate the file (start fresh)
            open(log_filename, 'w').close()

    # Create logger
    logger = logging.getLogger(scraper_name)
    logger.setLevel(logging.INFO)

    # Clear existing handlers to avoid duplicate logs
    logger.handlers.clear()

    # Create file handler (append mode)
    file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create formatter
    log_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler.setFormatter(log_formatter)
    console_handler.setFormatter(log_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Logger initialized for {scraper_name} scraper")
    logger.info(f"Log file: {log_filename}")

    return logger

def log_scraper_progress(logger, stage, details="", match_count=0, error=None):
    """
    Log scraper progress with consistent format

    :param logger: logger object
    :param stage: str - current stage (e.g., 'START', 'PARSING', 'MATCHING', 'BULK_UPDATE')
    :param details: str - additional details
    :param match_count: int - number of matches processed
    :param error: Exception - error object if any
    """
    if error:
        logger.error(f"[{stage}] ERROR: {str(error)} | Details: {details}")
    else:
        message = f"[{stage}]"
        if match_count > 0:
            message += f" Matches: {match_count}"
        if details:
            message += f" | {details}"
        logger.info(message)


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
            '%d %b %Y %H:%M:%S',  # "25 Dec 2024 15:30:00"
            '%Y-%m-%d %H:%M:%S',  # "2024-12-25 15:30:00"
            '%Y-%m-%dT%H:%M:%S%z',  # "2024-12-25T15:30:00+00:00"
            '%Y-%m-%dT%H:%M:%SZ',  # "2024-12-25T15:30:00Z"
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


def execute_bulk_write_operations(collection, bulk_operations, operation_type="update", logger=None):
    """
    Execute bulk write operations with logging

    :param collection: MongoDB collection
    :param bulk_operations: list of bulk operations
    :param operation_type: str - type of operation (update, insert, etc.)
    :param logger: logger object
    :return: bulk write result
    """
    if not bulk_operations:
        if logger:
            logger.warning(f"No bulk {operation_type} operations to execute")
        return None

    try:
        if logger:
            logger.info(f"Executing {len(bulk_operations)} bulk {operation_type} operations")

        bulk_result = collection.bulk_write(bulk_operations)

        if logger:
            logger.info(f"Bulk {operation_type} completed successfully:")
            logger.info(f"  - Modified: {getattr(bulk_result, 'modified_count', 0)}")
            logger.info(f"  - Upserted: {getattr(bulk_result, 'upserted_count', 0)}")
            logger.info(f"  - Inserted: {getattr(bulk_result, 'inserted_count', 0)}")

        return bulk_result

    except Exception as bulk_error:
        if logger:
            logger.error(f"Bulk {operation_type} error: {bulk_error}")
        raise bulk_error


def store_data_into_mongodb(matches_data, database_name, logger=None):
    """
    Store match data into MongoDB with consistent timestamp handling and bulk operations
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
        execute_bulk_write_operations(collection, operations, "upsert", logger)


def store_competitor_mapping_data(docs, logger=None):
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
        result = execute_bulk_write_operations(collection, operations, "competitor_mapping", logger)
        if logger:
            logger.info(f"Competitor mapping bulk write completed: {result.bulk_api_result}")


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


def check_key(name):
    not_used_key = ['scoreless','end','go', 'never', 'niether', 'total runs', 'to score', 'get', 'including overtime', 'own', 'retain',
                    ':', '0:', 'award', 'kick',
                    'penalty', 'target', 'shots', 'series', 'yellow', 'super over',
                    'header', '4s', 'touchdown', 'of the match', 'hero', 'tries', 'more', '6s', 'fifty', 'scored',
                    'century', 'last', "player's total runs", 'given', 'red', 'retain', 'most', 'odd/even',
                    'performance', 'converted', 'listed', 'center', 'margin', 'try', 'either', 'hc ', 'runs ',
                    'rushing', 'out', 'yards', 'receving', 'both', 'and', 'remaining', '(', ')', 'tie', 'break',
                    'deuce', 'next', 'touchdowns', 'range', 'will', '?', 'did', 'does', 'hour', 'minute', 'halves',
                    'scorer',
                    'betting', '&', '1 .ht', '1. ht', '1.ht', 'number of runs in match', 'how', 'which', 'who',
                    'result', 'win', 'halftime', 'legs', 'half time', 'full time',
                    'tackles', 'attempts', 'final', 'frame', 'side', 'wides', 'highest', 'four',
                    'sixes', 'assists', 'made', 'home', 'away', 'rebounds', 'milestones', 'qualify', 'exact',
                    'bottom', 'top', 'wicket', 'at least', 'at end', 'at the end', 'before', 'after', 'fulltime',
                    'lead', 'race', 'stats', 'specials', 'squares', 'puck', 'record']

    if not any(word in name.lower() for word in not_used_key):
        if '2-way & over/under' in name.lower():
            return False
        if 'half' in name.lower():
            if 'point spread' in name.lower() and 'o/u' in name.lower():
                return False
            if 'winner' in name.lower():
                return False
            return True
        elif 'first' not in name.lower():
            if 'point spread' in name.lower() and 'o/u' in name.lower():
                return False
            if 'winner' in name.lower():
                return False
            return True
    return False


def check_sport_name(sport_name):
    sport_name = sport_name.replace('-', '').replace(' ', '').lower()
    all_sport_name = ['rugby', 'football', 'soccer', 'tennis', 'basketball', 'hockey', 'americanfootball', 'baseball',
                      'handball', 'rugbyunion',
                      'floorball', 'bandy', 'futsal', 'volleyball', 'cricket', 'snooker', 'beachvolleyball',
                      'aussierules',
                      'rugbyleague', 'badminton', 'waterpolo', 'fieldhockey', 'tabletennis', 'beachsoccer', 'netball',
                      'pesapallo',
                      'kabaddi']
    # all_sport_name = ['soccer']
    if sport_name in all_sport_name:
        return True
    return False


def check_header_name(key):
    original_key = key
    conversion_list = ['-half-1', '-half-2', '-one-half', '-period-1', '-period-2', '-period-3', '-set-1', '-set-2',
                       '-set-3', '-set-4', '-set-5', '-quarter-1', '-quarter-2', '-quarter-3', '-quarter-4',
                       '-inning-1',
                       '-inning-2', '-inning-3', '-inning-4', '-inning-5', '-inning-6', '-inning-7', '-inning-8',
                       '-inning-9']
    for repl in conversion_list:
        original_key = original_key.replace(repl, '')

    checked = ['top', 'wicket', 'halftime', 'at least', 'at end', 'at the end',
               'before', 'after', 'fulltime', 'lead']
    if not any(word in key.lower() for word in checked):
        half_data = ['half', '. h', ' .h']
        key = key.replace(' ', '').replace('-', '')
        if any(word.replace(' ', '').replace('-', '') in key.lower() for word in half_data):
            h1 = ['1sthalf', 'firsthalf', '1h', '1h', '1h',
                  'halfnumber1', 'halfno1', 'half1', '1half']
            h2 = ['2ndhalf', 'secondhalf', '2h', '2h', '2h',
                  'halfnumber2', 'halfno2', 'half2', '2half']
            if any(word.replace("-", "").replace(" ", "") in key.lower() for word in h1):
                if 'first half' in key.lower():
                    if key.lower().count('first') == 1:
                        key_name = '1st Half'


                    else:
                        key_name = 'Full Match'
                elif 'first' in key.lower():
                    key_name = 'Full Match'
                else:
                    key_name = '1st Half'



            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     h2) and 'first' not in key.lower():
                key_name = '2nd Half'

            else:
                key_name = 'Full Match'

        elif 'quarter' in key.lower():
            q1 = ['1stquarter', 'quarter1', 'quarterone', 'firstquarter',
                  '1quarter', 'quarterno1', 'quarternumber1',
                  'quarter no.1']
            q2 = ['2ndquarter', 'quarter2', 'quartertwo', 'secondquarter',
                  '2quarter', 'quarterno2', 'quarternumber2',
                  'quarter no.2']
            q3 = ['3rdquarter', 'quarter3', 'quarterthree', 'thirdquarter',
                  '3quarter', 'quarterno3', 'quarternumber3',
                  'quarter no.3']
            q4 = ['4thquarter', 'quarter4', 'quarterfour', 'fourthquarter',
                  '4quarter', 'quarterno4', 'quarternumber4',
                  'quarter no.4']
            if any(word.replace("-", "").replace(" ", "") in key.lower() for word in q1):
                if 'first quarter' in key.lower():
                    if key.lower().count('first') == 1:
                        key_name = '1st Quarter'

                    else:
                        key_name = 'Full Match'
                elif 'first' in key.lower():
                    key_name = 'Full Match'

                else:
                    key_name = '1st Quarter'



            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     q2) and 'first' not in key.lower():
                key_name = '2nd Quarter'

            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     q3) and 'first' not in key.lower():
                key_name = '3rd Quarter'

            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     q4) and 'first' not in key.lower():
                key_name = '4th Quarter'

            else:
                key_name = 'Full Match'
        elif 'set' in key.lower():
            s1 = ['1st set', 'set 1', 'set one', 'first set', 'set no. 1', 'set number 1',
                  '1.set', '1 .set', '1. set', 'set no.1']
            s2 = ['2nd set', 'set 2', 'set two', 'second set', 'set no. 2', 'set number 2',
                  '2.set', '2 .set', '2. set', 'set no.2']
            s3 = ['3rd set', 'set 3', 'set three', 'third set', 'set no. 3', 'set number 3',
                  '3.set', '3 .set', '3. set', 'set no.3']
            s4 = ['4th set', 'set 4', 'set four', 'fourth set', 'set no. 4', 'set number 4',
                  '4.set', '4 .set', '4. set', 'set no.4']
            s5 = ['5th set', 'set 5', 'set five', 'fifth set', 'set no. 5', 'set number 5',
                  '5.set', '5 .set', '5. set', 'set no.5']
            if any(word.replace("-", "").replace(" ", "") in key.lower() for word in s1):
                if 'first set' in key.lower():
                    if key.lower().count('first') == 1:
                        key_name = '1st Set'

                    else:
                        key_name = 'Full Match'
                elif 'first' in key.lower():
                    key_name = 'Full Match'
                else:
                    key_name = '1st Set'


            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     s2) and 'first' not in key.lower():
                key_name = '2nd Set'

            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     s3) and 'first' not in key.lower():
                key_name = '3rd Set'

            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     s4) and 'first' not in key.lower():
                key_name = '4th Set'

            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     s5) and 'first' not in key.lower():
                key_name = '5th Set'

            else:
                key_name = 'Full Match'



        elif 'inning' in key.lower():

            i1 = ['1st inning', 'first inning', 'one inning', 'inning 1', 'inning one',
                  'inning no. 1', 'inning number 1', 'inning no.1']
            i2 = ['2nd inning', 'second inning', 'two inning', 'inning 2', 'inning two',
                  'inning no. 2', 'inning number 2', 'inning no.2']
            i3 = ['3rd inning', 'third inning', 'three inning', 'inning 3', 'inning third',
                  'inning no. 3', 'inning number 3', 'inning no.3']
            i4 = ['4th inning', 'fourth inning', 'four inning', 'inning 4', 'inning fourth',
                  'inning no. 4', 'inning number 4', 'inning no.4']
            i5 = ['5th inning', 'fifth inning', 'five inning', 'inning 5', 'inning fifth',
                  'inning no. 5', 'inning number 5', 'inning no.5']
            i6 = ['6th inning', 'sixth inning', 'six inning', 'inning 6', 'inning sixth',
                  'inning no. 6', 'inning number 6', 'inning no.6']
            i7 = ['7th inning', 'seventh inning', 'seven inning', 'inning 7',
                  'inning seventh', 'inning no. 7', 'inning number 7', 'inning no.7']
            i8 = ['8th inning', 'eighth inning', 'eight inning', 'inning 8',
                  'inning eighth', 'inning no. 8', 'inning number 8', 'inning no.8']
            i9 = ['9th inning', 'ninth inning', 'nine inning', 'inning 9', 'inning ninth',
                  'inning no. 9', 'inning number 9', 'inning no.9']
            if any(word.replace("-", "").replace(" ", "") in key.lower() for word in i1):
                if 'first inning' in key.lower():
                    if key.lower().count('first') == 1:
                        key_name = '1st Inning'

                    else:
                        key_name = 'Full Match'
                elif 'first' in key.lower():
                    key_name = 'Full Match'
                else:
                    key_name = '1st Inning'

            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     i2) and 'first' not in key.lower():
                key_name = '2nd Inning'


            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     i3) and 'first' not in key.lower():
                key_name = '3rd Inning'


            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     i4) and 'first' not in key.lower():
                key_name = '4th Inning'


            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     i5) and 'first' not in key.lower():
                key_name = '5th Inning'


            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     i6) and 'first' not in key.lower():
                key_name = '6th Inning'


            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     i7) and 'first' not in key.lower():
                key_name = '7th Inning'


            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     i8) and 'first' not in key.lower():
                key_name = '8th Inning'


            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     i9) and 'first' not in key.lower():
                key_name = '9th Inning'


            else:
                key_name = 'Full Match'


        elif 'period' in key.lower():

            p1 = ['1st period', 'first period', 'one period', 'period 1', 'period one',
                  'period no. 1', 'period number 1', 'period no.1']
            p2 = ['2nd period', 'second period', 'two period', 'period 2', 'period two',
                  'period no. 2', 'period number 2', 'period no.2']
            p3 = ['3rd period', 'third period', 'three period', 'period 3', 'period third',
                  'period no. 3', 'period number 3', 'period no.3']

            if any(word.replace("-", "").replace(" ", "") in key.lower() for word in p1):
                if 'first period' in key.lower():
                    if key.lower().count('first') == 1:
                        key_name = '1st period'

                    else:
                        key_name = 'Full Match'
                elif 'first' in key.lower():
                    key_name = 'Full Match'
                else:
                    key_name = '1st period'

            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     p2) and 'first' not in key.lower():
                key_name = '2nd period'


            elif any(word.replace("-", "").replace(" ", "") in key.lower() for word in
                     p3) and 'first' not in key.lower():
                key_name = '3rd period'


            else:
                key_name = 'Full Match'
        else:
            key_name = 'Full Match'

    else:
        key_name = 'Full Match'
    return [key_name, original_key]


def compare_matchups(
        team1_a: str,
        team2_a: str,
        team1_b: str,
        team2_b: str,
        threshold: float = 80.0,
) -> bool:
    sim_team1_sort = fuzz.token_sort_ratio(team1_a, team1_b)
    sim_team2_sort = fuzz.token_sort_ratio(team2_a, team2_b)
    sim_team1_set = fuzz.token_set_ratio(team1_a, team1_b)
    sim_team2_set = fuzz.token_set_ratio(team2_a, team2_b)
    combined_a = f"{team1_a} {team2_a}"
    combined_b = f"{team1_b} {team2_b}"
    sim_combined_sort = fuzz.token_sort_ratio(combined_a, combined_b)
    sim_combined_set = fuzz.token_set_ratio(combined_a, combined_b)

    result = {
        'flashcore_team1': team1_a,
        'tipico_team1': team1_b,
        'flashcore_team2': team2_a,
        'tipico_team2': team2_b,
        'is_match': '',
        'team1_score_sort': sim_team1_sort,
        'team2_score_sort': sim_team2_sort,
        'is_match_sort': sim_team1_sort >= threshold and sim_team2_sort >= threshold,
        'combined_score_sort': sim_combined_sort,
        'is_match_combined_sort': True if sim_combined_sort >= 80 else False,
        'team1_score_set': sim_team1_set,
        'team2_score_set': sim_team2_set,
        'is_match_set': sim_team1_set >= threshold and sim_team2_set >= threshold,
        'combined_score_set': sim_combined_set,
        'is_match_combined_set': True if sim_combined_set >= 80 else False
    }

    # return result
    check = True if sim_combined_set >= 80 else False
    return check
def remove_empty_dicts(obj):
    """
    Recursively removes:
    1. Completely empty dicts {}
    2. Dicts where all values are empty strings, only spaces, or None
    """
    if isinstance(obj, dict):
        # First, clean nested values
        cleaned = {k: remove_empty_dicts(v) for k, v in obj.items()}

        # Remove keys whose values became None after cleaning
        cleaned = {k: v for k, v in cleaned.items() if v is not None}

        # Check if dict is completely empty
        if not cleaned:
            return None

        # Check if all values are empty string, space string, or None
        if all((v is None) or (isinstance(v, str) and v.strip() == '') for v in cleaned.values()):
            return None

        return cleaned

    elif isinstance(obj, list):
        # Process each element in the list
        cleaned_list = [remove_empty_dicts(v) for v in obj]
        # Remove None values from the list
        cleaned_list = [v for v in cleaned_list if v is not None]
        return cleaned_list

    else:
        return obj