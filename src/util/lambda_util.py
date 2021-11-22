"""Utilities for lambda functions."""
import boto3
import json
import traceback
import sys
import time
import decimal
from . import ddb_util, LOGGER, errors, py_util


def timed_func(func):
    """Decorator that logs the time a function call takes.

    TODO: hook this up to a reporting mechanism so we can profile functions.
    """
    def inner_wrap(*args, **kwargs):
        LOGGER.info('%s called...' % func.__name__)
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        time_taken = end - start
        LOGGER.info('%s completed in %ss.' % (func.__name__, time_taken))
        return result
    return inner_wrap


def error_500():
    # for unexpected errors
    LOGGER.info('Lambda encountered an unexpected error.')

    return {'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(
                {'data': {},
                 'errorMessage': 'Unexpected error.'},
                cls=py_util.DecimalEncoder)}


def error_400(body):
    # for expected errors
    LOGGER.info('Lambda encountered an expected error.')

    # body is a dictionary - will be stringified here
    return {'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(
                {'data': {},
                 'errorMessage': body},
                cls=py_util.DecimalEncoder)}


def message(msg):
    # for not an error, just info, but different from an OK
    LOGGER.info('Lambda encountered a problem, returning error message...')

    return {'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(
                {'message': msg,
                 'data': {},
                 'errorMessage': None}
            )}


def ok(body):
    # for good response
    LOGGER.info('Lambda finished successfully, returning result...')

    # body is a dictionary - will be stringified here
    return {'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(
                {'data': body,
                 'errorMessage': None},
                cls=py_util.DecimalEncoder)}


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


class Lambda:
    """Wrapper for a lambda function.

    This wrapper motivated by code reuse and efficiency.
    """

    def __init__(self, event, context):
        """Create a new Lambda.

        Args:
          event: Dictionary.
          context: LambdaContext object.
        """
        self.logger = LOGGER
        self.event = event
        self.context = context
        self.log_call()
        self.check_params()
        # initialize dynamodb resources
        if 'test' in event.keys():  # for testing
            self.ddb = boto3.resource(
                'dynamodb', endpoint_url='http://localhost:8000')
            self.client = boto3.client(
                'dynamodb', endpoint_url='http://localhost:8000')
        else:
            self.ddb = boto3.resource('dynamodb')
            self.client = boto3.client('dynamodb')
        self.query_paginator = self.client.get_paginator('query')
        self.scan_paginator = self.client.get_paginator('scan')

    def __call__(self):
        """Call to Lambda function.

        Returns:
          Dictionary consistent with lambda proxy integration.
        """
        try:
            return self.function_logic()

        except Exception as e:
            # check for any expected errors
            if isinstance(e, errors.Error):  # i.e. one we expect
                self.logger.error('Encountered an expected error')
                self.logger.error(type(e))
                self.logger.error(e.error_message)
                if e._type == '400':
                    return self.error_400(e.error_message)
                elif e._type == 'info':
                    return self.message(e.error_message)
                else:
                    self.logger.error('Unexpected error type %r' % e._type)
                    return self.error_500()
            # otherwise it is unexpected - return 500 error
            else:
                self.logger.error('Encountered unknown error.')
                _type, _value, _traceback = sys.exc_info()
                self.logger.error('Type: %s' % _type)
                self.logger.error('Value: %s' % _value)
                self.logger.error('Traceback:')
                for x in traceback.format_tb(_traceback):
                    self.logger.error(x)
                return self.error_500()
        finally:
            self._finally()

    def _finally(self):
        return

    @staticmethod
    def required_params():
        return []

    def get_param(self, param):
        return self.event['pathParameters'][param]

    @timed_func
    def scan(self, table_name, projection=None, _filter=None, attr_names=None,
             attr_values=None, max_items=None, drop_missing_attrs=True,
             index_name=None, capacity=None):
        # attr_values is a dict, not object
        table = self.ddb.Table(table_name)
        page_size, num_pages = ddb_util.records_per_second(
            table, self.logger, capacity=capacity)
        pagination_config = {'PageSize': page_size}
        if max_items:
            pagination_config['MaxItems'] = max_items
        args = {'TableName': table_name,
                'PaginationConfig': pagination_config}
        if index_name:
            args['IndexName'] = index_name
        if projection:
            args['Select'] = 'SPECIFIC_ATTRIBUTES'
            args['ProjectionExpression'] = projection
        else:
            args['Select'] = 'ALL_ATTRIBUTES'
        if _filter:
            args['FilterExpression'] = _filter
        if attr_names:
            args['ExpressionAttributeNames'] = attr_names
        if attr_values:
            args['ExpressionAttributeValues'] = attr_values
        self.logger.info('Scanning table %s with args:' % table_name)
        self.logger.info(args)
        items = self.pagination(self.scan_paginator, args, page_size)
        self.logger.info('Scan returned %s items:'
                         % len(items))
        items = [ddb_util.parse_result(item) for item in items]
        if projection and drop_missing_attrs:
            items = self.drop_missing_attrs(items, attr_names, projection)
        self.logger.info(items)
        return items

    @timed_func
    def query(self, table_name, condition, attr_values, projection=None,
              attr_names=None, drop_missing_attrs=True, index_name=None,
              capacity=None):
        # attr_values is a dict, not object
        table = self.ddb.Table(table_name)
        page_size, num_pages = ddb_util.records_per_second(
            table, self.logger, capacity=capacity)
        args = {
            'TableName': table_name,
            'PaginationConfig': {'PageSize': page_size},
            'Select': 'SPECIFIC_ATTRIBUTES',
            'KeyConditionExpression': condition,
            'ExpressionAttributeValues': attr_values}
        if index_name:
            args['IndexName'] = index_name
        if projection:
            args['Select'] = 'SPECIFIC_ATTRIBUTES'
            args['ProjectionExpression'] = projection
        else:
            args['Select'] = 'ALL_ATTRIBUTES'
        if attr_names:
            args['ExpressionAttributeNames'] = attr_names
        self.logger.info('Querying table %s with args:' % table_name)
        self.logger.info(args)
        items = self.pagination(
            self.query_paginator, args, page_size)
        items = self.parse_items(items)
        if projection and drop_missing_attrs:
            items = self.drop_missing_attrs(items, attr_names, projection)
        self.logger.info(items)
        return items

    def pagination(self, paginator, args, page_size):
        items = []
        for page in paginator.paginate(**args):
            if page['Count'] > 0:
                items += page['Items']
            if page['Count'] == page_size:
                time.sleep(1)
        self.logger.info('Query returned %s items:'
                         % len(items))
        return items

    @timed_func
    def parse_items(self, items):
        items = [ddb_util.parse_result(item) for item in items]
        self.logger.info(items)
        return items

    def drop_missing_attrs(self, items, attr_names, projection):
        self.logger.info('Dropping items without all attributes...')
        attrs = [(attr_names[a]
                 if (attr_names is not None and a in attr_names.keys())
                 else a).strip()  # take care of errant spaces
                 for a in projection.split(',')]
        self.logger.info('Attributes required:')
        self.logger.info(attrs)
        items = [i for i in items if all(a in i.keys() for a in attrs)]
        self.logger.info('%s items remain:' % len(items))
        return items

    def query_one(self, table_name, condition, attr_values, projection=None,
                  attr_names=None, drop_missing_attrs=False):
        items = self.query(
            table_name=table_name, projection=projection, condition=condition,
            attr_values=attr_values, attr_names=attr_names,
            drop_missing_attrs=drop_missing_attrs)
        if len(items) == 0:
            self.logger.error('No records returned!')
            raise errors.NoRecord('No records returned.')
        if len(items) > 1:
            self.logger.error('More than one record returned!')
            raise errors.MoreThanOneRecord('More than one record returned.')
        return items[0]

    def query_one_or_none(self, table_name, projection, condition, attr_values,
                          attr_names=None):
        items = self.query(
            table_name=table_name, projection=projection, condition=condition,
            attr_values=attr_values, attr_names=attr_names)
        if len(items) > 1:
            self.logger.error('More than one record returned!')
            raise errors.MoreThanOneRecord()
        if len(items) == 1:
            return items[0]
        else:
            return None

    def count(self, table_name, condition, attr_values):
        # attr_values is an object, not dict
        self.logger.info('Counting %s with condition %s'
                         % (table_name, condition))
        table = self.ddb.Table(table_name)
        response = table.query(
            Select='COUNT',
            KeyConditionExpression=condition,
            ExpressionAttributeValues=attr_values)
        self.logger.info('Response for count:')
        self.logger.info(response)
        return response['Count']

    def log_call(self):
        # logs basic information about the call to the lambda
        self.logger.info('Call to Lambda...')
        self.logger.info('Event:')
        self.logger.info(self.event)
        self.logger.info('Context:')
        self.logger.info(self.context)
        self.logger.info('Path: %r' % self.event['path'])

    def check_params(self):
        # none by default
        pass

    def check_key_in_params(self, key_name):
        if key_name not in self.event['pathParameters'].keys() \
                or self.event['pathParameters'][key_name] is None:
            msg = '"%s" not in pathParameters.' % key_name
            self.logger.error(msg)
            raise ValueError(msg)

    @timed_func
    def function_logic(self):
        # must be overridden
        raise NotImplementedError

    @staticmethod
    def error_500():
        # for unexpected errors
        LOGGER.info('Lambda encountered an unexpected error.')

        return {'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(
                    {'data': {},
                     'errorMessage': 'Unexpected error.'}, cls=DecimalEncoder)}

    @staticmethod
    def error_400(body):
        # for expected errors
        LOGGER.info('Lambda encountered an expected error.')

        # body is a dictionary - will be stringified here
        return {'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(
                    {'data': {},
                     'errorMessage': body}, cls=DecimalEncoder)}

    @staticmethod
    def message(msg):
        # for not an error, just info, but different from an OK
        LOGGER.info('Lambda encountered a problem, returning error message...')

        return {'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(
                    {'message': msg,
                     'data': {},
                     'errorMessage': None}
                )}

    @staticmethod
    def ok(body):
        # for good response
        LOGGER.info('Lambda finished successfully, returning result...')

        # body is a dictionary - will be stringified here
        return {'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(
                    {'data': body,
                     'errorMessage': None}, cls=DecimalEncoder)}


class Lambda2(object):

    def __init__(self, logger=LOGGER):
        self.logger = logger

    def __call__(self, event, context):
        """Call to Lambda function.

        Returns:
          Dictionary consistent with lambda proxy integration.
        """
        try:
            self.log_call(event, context)
            self.check_params(event)
            if 'profile_name' in event.keys():
                profile_name = event['profile_name']
            else:
                profile_name = 'local' if 'test' in event.keys() else None
            self.profile_name = profile_name
            self.capacity = event['capacity'] \
                if 'capacity' in event.keys() \
                else None
            self.n_threads = event['n_threads'] \
                if 'n_threads' in event.keys() \
                else 1
            self.ddb = ddb_util.LambdaDDB(profile_name)
            return self.function_logic(event, context)

        except Exception as e:
            # check for any expected errors
            if isinstance(e, errors.Error):  # i.e. one we expect
                self.logger.error('Encountered an expected error')
                self.logger.error(type(e))
                self.logger.error(e.error_message)
                if e._type == '400':
                    return error_400(e.error_message)
                elif e._type == 'info':
                    return message(e.error_message)
                else:
                    self.logger.error('Unexpected error type %r' % e._type)
                    return error_500()
            # otherwise it is unexpected - return 500 error
            else:
                self.logger.error('Encountered unknown error.')
                _type, _value, _traceback = sys.exc_info()
                self.logger.error('Type: %s' % _type)
                self.logger.error('Value: %s' % _value)
                self.logger.error('Traceback:')
                for x in traceback.format_tb(_traceback):
                    self.logger.error(x)
                return error_500()
        finally:
            self._finally()

    def _finally(self):
        return

    def log_call(self, event, context):
        # logs basic information about the call to the lambda
        self.logger.info('Call to Lambda: %s' % self.__class__.__name__)
        self.logger.info('Event:')
        self.logger.info(event)
        self.logger.info('Context:')
        self.logger.info(context)
        self.logger.info('Path: %r' % event['path'])

    def check_params(self, event):
        for key_name in self.expected_path_params():
            self.check_key_in_params(event, key_name)

    def check_key_in_params(self, event, key_name):
        if key_name not in event['pathParameters'].keys() \
                or event['pathParameters'][key_name] is None:
            msg = '"%s" not in pathParameters.' % key_name
            self.logger.error(msg)
            raise ValueError(msg)

    def expected_path_params(self):
        return []

    @timed_func
    def function_logic(self, event, context):
        raise NotImplementedError

    def get_path_parameters(self, event):
        return (event['pathParameters'][p] for p in self.expected_path_params())
