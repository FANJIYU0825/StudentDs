"""General python utilities."""
import sys
import json
import time
import decimal
import zipfile
import datetime
import traceback
from . import LOGGER


def average(sigma, n):
    if n == 0:
        return 0
    return int(round(sigma / float(n), 0))


def avg(s, n):
    if n == 0:
        return 0
    return s / float(n)


def chunk(_list, chunk_size):
    """Yield successive n-sized chunks of a list.

    Args:
      _list: List of objects.
      chunk_size: Integer.

    Returns:
      Generator.
    """
    for i in range(0, len(_list), chunk_size):
        yield _list[i:i + chunk_size]


def flatten(list_of_lists):
    """Flattens a list of lists into a single flat list.

    Args:
      list_of_lists: List of lists.

    Returns:
      List.
    """
    return [item for sub_list in list_of_lists for item in sub_list]


def json_serial(obj):
    # https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable
    # jgbarah's answer
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def pad(i):
    """Convert an Integer to String padding with zeros to two characters.

    Args:
      i: Integer.

    Returns:
      String.
    """
    s = str(i)
    if len(s) == 1:
        return '0' + s
    return s


def print_exception_info():
    """Grabs traceback and prints exception info."""
    print('Encountered an exception:')
    type_, value_, traceback_ = sys.exc_info()
    print(f'Type:{ type_}')
    print(f'Value: { value_}')
    print('Traceback:')
    for line in traceback.format_tb(traceback_):
        print(line)


def sub_dict(dictionary, attrs):
    values = (dictionary[attr] for attr in attrs)
    return dict(zip(attrs, values))


def timed_func(func):
    """Decorator that logs the time a function call takes.

    TODO: hook this up to a reporting mechanism so we can profile functions.
    """
    def inner_wrap(*args, **kwargs):
        LOGGER.debug('%s called...' % func.__name__)
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        time_taken = end - start
        LOGGER.debug('%s completed in %ss.' % (func.__name__, time_taken))
        return result
    return inner_wrap


def zip_data(data, file_name):
    with zipfile.ZipFile(file_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        with open('src/temp/data.json', 'w') as f:
            _json = json.dumps(data, cls=DecimalEncoder)
            f.write(_json)
        zipf.write('src/temp/data.json', 'data.json')
    os.remove('src/temp/data.json')


class DecimalEncoder(json.JSONEncoder):
    """For dumping python decimals to JSON.

    Taken from Elias Zamaria's answer here:
    https://stackoverflow.com/questions/1960516/python-json-serialize-a-decimal-object

    Usage:
    json.dumps(decimal.Decimal('10.0'), cls=DecimalEncoder).
    """
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
