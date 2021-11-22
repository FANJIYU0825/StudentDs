"""DynamoDB specific utilities."""
import os
import math
import time
import json
import boto3
import pickle
import decimal
import datetime
from boto3.dynamodb import types
from . import LOGGER, py_util, errors


"""
The following are definitions of our table schemas in DynamoDB.

Quick reference for AttributeTypes:

  S: string
  N: any number type
  BOOL: obvious innit
  B: binary
  S: date (stored as ISO-8601 formatted strings
  SS: string set
  NS: number set
  BS: binary set

Reference:
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/
DynamoDBMapper.DataTypes.html.
"""


# [PPK, PSK] for each table
DDB_KEYS = {
    'activities-completed': [('code_pin', 'S'), (None, None)],
    'activity-history': [('code_pin', 'S'), ('date', 'N')],
    'class-code-counters': [('name', 'S'), (None, None)],
    'classes': [('username', 'S'), ('code', 'S')],
    'experience': [('cls_loc', 'S'), ('date', 'N')],
    'hourly-stats': [('cls', 'S'), ('usr', 'S')],
    'school-groups': [('name', 'S'), (None, None)],
    'schools': [('groupName', 'S'), ('name', 'S')],
    'student-assignments': [('class_code', 'S'), ('student_pin', 'S')],
    'students': [('class_code', 'S'), ('student_pin', 'S')],
    'teachers': [('group::school', 'S'), ('username', 'S')],
    'user-accounts': [('username', 'S'), (None, None)]
}


# list of DynamoDB reserved keywords we encounter with our attribute names
RESERVED = ['date', 'name', 'location']  # TODO: find the others


# currently only used with aggs, but is a ddb_util concept
ID_ATTR = {
    'classes': 'code',
    'teachers': 'username',
    'schools': 'name',
    'school-groups': 'name'
}


# currently only used with aggs, but is a ddb_util concept
PARENT_ID_ATTR = {
    'students': 'class_code',
    'classes': 'username',
    'teachers': '#gs',
    'schools': 'groupName'
}


def obj_key(obj, _type):
    if _type == 'class-code-counters':
        return {'name': obj['name']}

    elif _type == 'classes':
        return {'username': obj['username'],
                'code': obj['code']}

    elif _type == 'experience':
        return {'cls_loc': obj['cls_loc'],
                'date': obj['date']}

    elif _type == 'hourly-stats':
        return {'cls': obj['cls'],
                'usr': obj['usr']}

    elif _type == 'school-groups':
        return {'name': obj['name']}

    elif _type == 'schools':
        return {'groupName': obj['groupName'],
                'name': obj['name']}

    elif _type == 'stats':
        return {'cls': obj['cls'], 'usr': obj['usr']}

    elif _type == 'student-assignments':
        return {'class_code': obj['class_code'],
                'student_pin': obj['student_pin']}

    elif _type == 'students':
        return {'class_code': obj['class_code'],
                'student_pin': obj['student_pin']}

    elif _type == 'teachers':
        return {'group::school': obj['group::school'],
                'username': obj['username']}

    elif _type == 'user-accounts':
        return {'username': obj['username']}

    else:
        raise ValueError('Unexpected _type %r' % _type)


def ddb_keys(table_name, date=datetime.date.today()):
    """Get PSK and PPK for a given table, including data types.

    Ostensibly this could be a dictionary but for the fact that the structure
    of the DB has, and presumably will, change with time. Therefore the return
    value should also be a function of time.

    Args:
      table_name: String.
      date: datetime.date. Defaults to datetime.date.today(). Exposed as an
        argument mainly for testing.

    Returns:
      Dictionary like {PSK: {type, name}, PPK: {type, name}}.

    Raises:
      ValueError if table_name not in DDB_KEYS.
    """
    if table_name not in DDB_KEYS.keys():
        raise ValueError('Unexpected table name: %r' % table_name)
    return dict(zip(['PPK', 'PSK'],
                    [{'attr': k[0], 'type': k[1]}
                     for k in DDB_KEYS[table_name]]))


def deserialize(items):
    deserializer = types.TypeDeserializer()
    #items = {k: deserializer.deserialize(v) for k, v in items.items()}
    items = [deserializer.deserialize(i) for i in items]
    LOGGER.info(items)
    return items


def avg(sigma, n, dp=4, zero_none=False):
    """Calculate an average that will play nicely with DynamoDB.

    Args:
      sigma: Decimal, the sum.
      n: Decimal, the number of records. Will be coerced to a float to ensure this
        calculates properly - due to an idiosyncracy of Python 2, if this is an
        integer this function will return 0.0.
      dp: Integer, number of decimal places. Default is 4.
      zero_none: Boolean, whether or not to return zero if there are no values
        to average. Otherwise will raise an exception.

    Returns:
      decimal.Decimal.

    Raises:
      ValueError if n == 0 and zero_none is False.
    """
    if n == 0 :
        if zero_none:
            return decimal.Decimal('0.')
        else:
            raise ValueError('Cannot take average of an empty sequence')
    sigma = decimal.Decimal(str(sigma))
    n = decimal.Decimal(str(n))
    return parse_decimal(sigma / n, dp)


def parse_decimal(f, dp=4):
    """Parse and round a float to a decimal that will play nicely with DynamoDB.

    Args:
      f: Float.
      dp: Integer, number of decimal places for rounding. Default is 4.

    Returns:
      decimal.Decimal.
    """
    return decimal.Decimal(str(round(f, dp)))


def parse_result(result):
    """Parse a result of a query or scan.

    E.g. we might get a value back in the value position
      {u'phone': {u'S': u'student1'}
    whereas we want
      {'phone': 'student1'}.
    This function returns this mapping. It can also handle nested dictionaries
    in the result.

    Args:
      result: Dictionary.

    Returns:
      Dictionary.
    """
    return {str(attr): parse_value(value) for attr, value in result.items()}


def parse_value(value):
    """Parses the value of an attribute returned from DynamoDB.

    E.g. we might get a value back in the value position
      {u'phone': {u'S': u'student1'}
    whereas we want
      {'phone': 'student1'}.
    This function maps {u'S': u'student1'} to 'student' in this case.

    Args:
      value: Dictionary like {type: value}.

    Returns:
      Object.

    Raises:
      ValueError if the type of the value is not expected.
    """
    _type = list(value.keys())[0]
    _value = list(value.values())[0]
    try:
        if _type == 'S':
            return _value.encode('utf-8')
        elif _type == 'BOOL':
            return bool(_value)
        elif _type == 'N':
            if '.' in _value:
                return decimal.Decimal(_value)
            else:
                return int(_value)
        elif _type == 'M':  # map case - recursion
            return parse_result(_value)
        elif _type == 'L':  # list case - recursion
            return [parse_value(item) for item in _value]
        elif _type == 'NULL':
            if isinstance(_value, bool):
                return _value
        else:
            raise ValueError('Unexpected value type %r' % _type)
    except Exception as e:
        print('raw value:')
        print(value)
        print('type:')
        print(_type)
        print('value:')
        print('%r' % _value)
        raise e


def parse_items(items):
    # TODO: there is native boto support for this:
    # deserializer = boto3.dynamodb.types.TypeDeserializer()
    # items = {k: deserializer.deserialize(v) for k, v in items.items()}
    items = [parse_result(item) for item in items]
    LOGGER.info(items)
    return items


def items_per_second(table, cushion=1., write=False, capacity=None,
                     eventually_consistent=True, index=None):
    """Determine how many items to safely take per second.

    From the AWS documentation:
      "A read capacity unit represents one strongly consistent read per second,
       or two eventually consistent reads per second, for an item up to 4 KB in
       size."

      "A write capacity unit represents one write per second, for an item up to
       1 KB in size."

      https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/
        ProvisionedThroughput.html#ProvisionedThroughput.CapacityUnits.Modifying

    The documentation says that eventually consistent is the default.

    Args:
      table: boto3.resources.factory.dynamodb.Table.
      cushion: Float. The average record size is multiplied by this to determine
        the safe to assume size per record. Defaults to 1.1.
      write: Bool. For a write operation set this to True.. For a read this
        should be False. Default is False.
      capacity: Int.
      eventually_consistent: Bool. If True each read unit counts twice. Default
        is True.
      index: String. If we are querying with a secondary index, we need to
        separately determine capacity. Default is None.

    Returns:
      items_per_second: Int.
      num_pages: Int. How many pages (i.e. seconds) to get all records in the
        table.
    """
    op = 'write' if write else 'read'
    LOGGER.debug('--- Determining %s items per second for table %s ---'
                 % (op, table.name))

    # determine average record size
    n_items = table.item_count
    LOGGER.debug('Table %s has %s items' % (table.name, n_items))
    if n_items > 0:  # need this for brand new tables
        avg_size = table.table_size_bytes / n_items / 1000.
    else:
        avg_size = 4  # default to one read capacity unit
        cushion = 1.  # don't apply cushioning in this case
    safe_size = avg_size * cushion  # in kb
    LOGGER.debug('Safe average size is %skb' % safe_size)

    # determine number of capacity units per record
    if write:
        units_per_item = int(math.ceil(safe_size))
    else:  # read
        units_per_item = int(math.ceil(safe_size / 4.))
    LOGGER.debug('One record costs %s %s unit%s'
                 % (units_per_item, op, 's' if units_per_item > 1 else ''))

    # determine table capacity
    if capacity is None:
        capacity_attr = 'WriteCapacityUnits' if write else 'ReadCapacityUnits'
        if index:
            gsi = next(i for i in table.global_secondary_indexes
                       if i['IndexName'] == index)
            capacity = gsi['ProvisionedThroughput'][capacity_attr]
        else:
            capacity = table.provisioned_throughput[capacity_attr]
    else:
        LOGGER.debug('Using overriden capacity %s' % capacity)
    if capacity == 0:
        LOGGER.debug('As needed provisioning - setting no limit.')
        capacity = 40000
    LOGGER.debug('Table %s%s is provisioned for %s %s units'
                 % (table.name,
                    (' (index %s)' % index) if index else '',
                    capacity,
                    op))

    # check if capacity is less than units per item
    if units_per_item > capacity:
        LOGGER.warn('Capacity for %s on %s is only %s, but each item requires'
                    ' %s units' % (op, table.name, capacity, units_per_item))
        return 1, n_items

    # determine items per second
    _items_per_second = capacity / units_per_item
    if not write and eventually_consistent:
        LOGGER.debug('Eventually consistent read units count as 2')
        _items_per_second *= 2
    LOGGER.debug('We are safe for %s %ss per second' % (_items_per_second, op))

    # determine number of pages for the whole table
    num_pages = int(math.ceil(n_items / float(_items_per_second)))
    LOGGER.debug('To %s the whole %s table we need %s pages'
                 % (op, table.name, num_pages))

    return _items_per_second, num_pages


def records_per_second(table, logger=LOGGER, cushion=1.1, write=False,
                       capacity=None, index=None):
    """Determine how many records to take per second given throughput limits.

    Amazon docs:
    https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/
        ProvisionedThroughput.html#ProvisionedThroughput.CapacityUnits.Modifying

    A read capacity unit allows one read of a record up to 4KB. If a record is
    greater than 4KB is it rounded up to the nearest multiple of 4KB. The
    smallest record it can consider is therefore 4KB. This is for "strongly
    consistent reads". If "eventually consistent" then halve that. The docs say
    the default mode is eventually consistent.

    This algorithm therefore performs the following calculations:
    - Get the average item size
    -

    Args:
      table: dynamoDB table.
      logger: a logger.
      cushion: Float, multiplies the average to give safe upper limit. Defaults
        to 1.5.
      write: Bool, if True then we look at write capacity, otherwise read.
        Default is False (i.e. read).
      capacity: Int, Kb per second. Default is None and this means the function
        will determine the capacity by reading the table. Specify a value to
        hard-code a value you want to work with - e.g. if working with auto-
        scaling. Remember that 1 capacity unit is 4Kb.

    TODO: get rid of the logger arg
    TODO: is there a way to find a more reliable value for cushion?

    Returns:
      page_size: Int.
      num_pages: Int.
    """
    LOGGER.warn('Deprecation warning - items_per_second should be used.')

    op = 'writing' if write else 'reading'
    logger.info('Determining records per second for %s %s'
                % (op, table.name))

    # hack to hard-code capacity for student-assignments for the minute
    # TODO: remove this hack
    if table.name == 'student-assignments':
        capacity = 200
        cushion = 1.

    # determine average record size
    size = table.table_size_bytes
    records = table.item_count
    if records > 0:  # need this for brand new tables
        avg_size = size / records / 1000.
    else:
        avg_size = 0.001
    safe_size = avg_size * cushion

    capacity_attr = 'WriteCapacityUnits' if write else 'ReadCapacityUnits'
    limit = table.provisioned_throughput[capacity_attr] * 4
    if capacity is not None:
        limit = capacity
    logger.info('%s provisioned for %skb/s (hard-coded=%s)'
                % (table.name, limit, capacity is not None))
    num_per_sec = max(2, int(limit / safe_size))  # int() floors as desired

    logger.info('%s KB / %s records = %s KB / record'
                % (size / 1000., records, avg_size))
    logger.info('Safe size: %s x %s = %s' % (avg_size, cushion, safe_size))
    logger.info('Safe records per sec: %s' % num_per_sec)

    if limit / safe_size < 1:
        logger.warning('Table "%s" safe page size exceeds limit!' % table.name)

    num_pages = math.ceil(records / num_per_sec)

    return int(num_per_sec), int(num_pages)


def get_projection(target_attrs):
    projection_attrs = []
    attr_names = {}
    for attr in target_attrs:
        if attr in RESERVED:  # handle reserved keywords
            pounded = '#%s' % attr
            projection_attrs.append(pounded)
            attr_names[pounded] = attr
        elif attr == 'group::school':  # this funky one
            projection_attrs.append('#gs')
            attr_names['#gs'] = 'group::school'
        else:  # other attrs with no issues
            projection_attrs.append(attr)
    return ','.join(projection_attrs), attr_names


class DDB(object):

    def __init__(self, profile, test=False, throttling=records_per_second,
                 logger=LOGGER):
        self.profile = profile
        self.throttling = throttling
        boto3.setup_default_session(profile_name=profile)
        if test:
            self.resource = boto3.resource(
                'dynamodb', endpoint_url='http://localhost:8000')
            self.client = boto3.client(
                'dynamodb', endpoint_url='http://localhost:8000')
            self.auto_scaling_client = boto3.client(
                'application-autoscaling', endpoint_url='http://localhost:8000')
        else:
            self.resource = boto3.resource('dynamodb')
            self.client = boto3.client('dynamodb')
            self.auto_scaling_client = boto3.client('application-autoscaling')
        self.query_paginator = self.client.get_paginator('query')
        self.scan_paginator = self.client.get_paginator('scan')
        self.logger = logger

    def table(self, table_name):
        return self.resource.Table(table_name)

    def empty(self, table_name, table_info):
        print('Emptying %s...' % table_name)
        if self.profile == 'china':
            raise Exception('You cannay delete China tables!')
        # https://stackoverflow.com/questions/28521631/empty-a-dynamodb-table-with-boto
        exists = table_name in [str(n) for n in
                                self.client.list_tables()['TableNames']]
        if exists:
            print('Deleting existing table...')
            self.client.delete_table(TableName=table_name)
        else:
            print('Table does not exist.')
        waiter = self.client.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)
        create_args = {
            'TableName': table_name,
            'KeySchema': table_info['key_schema'],
            'AttributeDefinitions': table_info['attribute_definitions'],
            'ProvisionedThroughput': {
                    k: v for k, v
                    in table_info['provisioned_throughput'].items()
                    if k in ['WriteCapacityUnits', 'ReadCapacityUnits']}}
        if table_info['local_secondary_indexes']:
            create_args['LocalSecondaryIndexes'] = \
                [{k: v for k, v in i.items()
                 if k in ['IndexName', 'Projection', 'KeySchema']}
                 for i in table_info['local_secondary_indexes']]
        if table_info['global_secondary_indexes']:
            create_args['GlobalSecondaryIndexes'] = \
                [{'IndexName': i['IndexName'],
                  'KeySchema': i['KeySchema'],
                  'Projection': i['Projection'],
                  'ProvisionedThroughput': {
                      'WriteCapacityUnits': i['ProvisionedThroughput'][
                          'WriteCapacityUnits'],
                      'ReadCapacityUnits': i['ProvisionedThroughput'][
                          'ReadCapacityUnits'],
                  }}
                 for i in table_info['global_secondary_indexes']]
        _ = self.resource.create_table(**create_args)
        waiter = self.client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        return self.table(table_name)

    def pagination(self, paginator, args, page_size, limit=None):
        # TODO: progress makes little sense since we don't know total beforehand
        items = []
        for page in paginator.paginate(**args):
            if page['Count'] > 0:
                items += page['Items']
            if limit:
                if len(items) >= limit:
                    items = items[:limit]
                    break
            if page['Count'] == page_size:
                time.sleep(1)
        self.logger.info('Query returned %s items:' % len(items))
        return items

    def delete_items(self, table_name, items, index=None):
        table = self.table(table_name)
        batch_size, num_batches = self.throttling(
            table, write=True, index=index)
        for batch in py_util.chunk(items, batch_size):
            for item in batch:
                key = obj_key(item, table_name)
                table.delete_item(Key=key)

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

    def query(self, table_name, condition, attr_values, projection=None,
              attr_names=None, drop_missing_attrs=True,
              index_name=None, override_capacity=None, reverse_order=False,
              limit=None):
        # attr_values is a dict, not object
        table = self.table(table_name)
        page_size, num_pages = self.throttling(
            table, index=index_name, capacity=override_capacity)
        args = {
            'TableName': table_name,
            'PaginationConfig': {'PageSize': page_size},
            'Select': 'SPECIFIC_ATTRIBUTES',
            'KeyConditionExpression': condition,
            'ExpressionAttributeValues': attr_values}
        if reverse_order:
            args['ScanIndexForward'] = False
        if projection:
            args['Select'] = 'SPECIFIC_ATTRIBUTES'
            args['ProjectionExpression'] = projection
        else:
            args['Select'] = 'ALL_ATTRIBUTES'
        if attr_names:
            args['ExpressionAttributeNames'] = attr_names
        if index_name:
            args['IndexName'] = index_name
        self.logger.info('Querying table %s with args:' % table_name)
        self.logger.info(args)
        items = self.pagination(
            self.query_paginator, args, page_size, limit=limit)
        items = parse_items(items)
        if projection and drop_missing_attrs:
            items = self.drop_missing_attrs(items, attr_names, projection)
        self.logger.info(items)
        return items

    def query_one(self, table_name, condition, attr_values, projection=None,
                  attr_names=None, drop_missing_attrs=False, index_name=None,
                  error_not_found=True):
        items = self.query(
            table_name=table_name, projection=projection, condition=condition,
            attr_values=attr_values, attr_names=attr_names,
            drop_missing_attrs=drop_missing_attrs, index_name=index_name)
        if len(items) == 0:
            if error_not_found:
                self.logger.error('No records returned!')
                raise errors.NoRecord('No records returned.')
            else:
                return None
        if len(items) > 1:
            self.logger.error('More than one record returned!')
            raise errors.MoreThanOneRecord('More than one record returned.')
        return items[0]

    def scan(self, table_name, projection=None, _filter=None, attr_names=None,
             attr_values=None, max_items=None, drop_missing_attrs=True,
             progress=None, override_capacity=None):
        # attr_values is a dict, not object
        table = self.table(table_name)
        page_size, num_pages = self.throttling(
            table, capacity=override_capacity)
        pagination_config = {'PageSize': page_size}
        if max_items:
            pagination_config['MaxItems'] = max_items
        args = {'TableName': table_name,
                'PaginationConfig': pagination_config}
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
        items = [parse_result(item) for item in items]
        if projection and drop_missing_attrs:
            items = self.drop_missing_attrs(items, attr_names, projection)
        self.logger.info(items)
        return items

    def update_provisioning(self, table_name, read=None, write=None, index=None,
                            keep_higher=False):
        # check arguments
        if not read and not write:
            raise ValueError('Must specify either a read or write capacity to'
                             'update.')

        # required variables
        attr = 'ReadCapacityUnits' if read else 'WriteCapacityUnits'
        target = read if read else write
        table = self.table(table_name)

        # log call info
        self.logger.info('Updating %s%s for %s to %s'
                         % (attr,
                            ' (index %s)' % index if index else '',
                            table_name,
                            target))

        # determine current provisioning
        current_read = self.get_base_provisioning(
            table_name=table_name,
            read=True,
            index_name=index)
        current_write = self.get_base_provisioning(
            table_name=table_name,
            read=False,
            index_name=index)
        current = current_read if read else current_write

        # check for the same (no action)
        if current == target:
            self.logger.warn('Trying to update provisioning to existing '
                             'value. No change made.')
            return

        # check for keep_higher
        if keep_higher and current >= target:
            self.logger.warn('Target provisioning lower than existing '
                             'and keep_higher=True. No change made.')
            return

        # determine new provisioning
        new_provisioning = {
            'ReadCapacityUnits': read if read else current_read,
            'WriteCapacityUnits': current_write if read else write}

        # wait for the table to be ACTIVE
        if index:
            while not self.gsi_status(table_name, index) == 'ACTIVE':
                pass
        else:
            while not table.table_status == 'ACTIVE':
                pass

        # perform update (is async)
        if index:
            self.client.update_table(
                TableName=table_name,
                GlobalSecondaryIndexUpdates=[{
                    'Update': {
                        'IndexName': index,
                        'ProvisionedThroughput': new_provisioning}}])
        else:
            self.client.update_table(
                TableName=table_name,
                ProvisionedThroughput=new_provisioning)

        # wait until update has finished
        self.logger.info('Waiting for provisioning update to take effect...')
        while not table.table_status == 'ACTIVE':
            pass

        start = time.time()

        # this can hang, and I don't know why - so set 20s waiting limit as a
        # default, seems to be OK
        while current != target and time.time() < (start + 20):
            current = self.get_base_provisioning(
                table_name=table_name,
                read=read is not None,
                index_name=index)

        # log results
        self.logger.info(
            'Table %s%s %s now at %s.'
                % (table_name,
                   (' (%s)' % index) if index else '',
                   attr,
                   current))

    def gsi_status(self, table_name, index_name):
        table = self.table(table_name)
        index = next(i for i in table.global_secondary_indexes
                     if i['IndexName'] == index_name)
        return index['IndexStatus']

    def get_ppk(self, table_name):
        table = self.table(table_name)
        return next(k for k in table.key_schema
                    if k['KeyType'] == 'HASH')['AttributeName']

    def get_psk(self, table_name):
        table = self.table(table_name)
        if len(table.key_schema) == 1:
            return None
        return next(k for k in table.key_schema
                    if k['KeyType'] == 'RANGE')['AttributeName']

    def get_base_provisioning(self, table_name, read=True, index_name=None):
        table = self.table(table_name)
        attr = 'ReadCapacityUnits' if read else 'WriteCapacityUnits'
        if index_name:
            index = next(i for i in table.global_secondary_indexes
                         if i['IndexName'] == index_name)
            return index['ProvisionedThroughput'][attr]
        else:
            return table.provisioned_throughput[attr]

    def get_auto_scaling(self, table_name, index_name=None, read=True):
        resource_id = 'table/%s' % table_name
        if index_name:
            resource_id += '/index/%s' % index_name
        targets = self.auto_scaling_client.describe_scalable_targets(
            ServiceNamespace='dynamodb')['ScalableTargets']
        dimension = 'ReadCapacityUnits' if read else 'WriteCapacityUnits'
        target = next(
            (t for t in targets
             if t['ResourceId'] == resource_id
             and dimension in t['ScalableDimension']),
            None)
        if target:
            return target['MinCapacity'], target['MaxCapacity']
        else:
            return None, None

    def update_auto_scaling(self, table_name, _min, _max, index_name=None):
        # NO, this should also be by action...
        policy_resources = [
            p['ResourceId'] for p in
            self.auto_scaling_client.describe_scaling_policies(
                ServiceNamespace='dynamodb')['ScalingPolicies']]
        if table_name in policy_resources:
            pass

    def get_provisioning(self, table_name):
        table = self.table(table_name)
        read_min, read_max = self.get_auto_scaling(table_name, read=True)
        write_min, write_max = self.get_auto_scaling(table_name, read=False)
        provisioning = {
            'read': {
                'base': table.provisioned_throughput['ReadCapacityUnits'],
                'min': read_min,
                'max': read_max},
            'write': {
                'base': table.provisioned_throughput['WriteCapacityUnits'],
                'min': write_min,
                'max': write_max}}
        for index_name in self.get_gsis(table_name):
            read_min, read_max = self.get_auto_scaling(
                table_name, index_name, read=True)
            write_min, write_max = self.get_auto_scaling(
                table_name, index_name, read=False)
            provisioning[index_name] = {
                'read': {
                    'base': self.get_base_provisioning(
                        table_name, index_name=index_name, read=True),
                    'min': read_min,
                    'max': read_max},
                'write': {
                    'base': self.get_base_provisioning(
                        table_name, index_name=index_name, read=False),
                    'min': write_min,
                    'max': write_max}}
        return provisioning

    def list_tables(self):
        return self.client.list_tables()['TableNames']

    def get_gsis(self, table_name):
        table = self.table(table_name)
        if table.global_secondary_indexes:
            return [i['IndexName'] for i in table.global_secondary_indexes]
        else:
            return []


def get_resources(profile_name=None):
    if profile_name:
        boto3.setup_default_session(profile_name=profile_name)
    if profile_name == 'local':
        resource = boto3.resource(
            'dynamodb', endpoint_url='http://localhost:8000')
        client = boto3.client(
            'dynamodb', endpoint_url='http://localhost:8000')
        auto_scaling_client = boto3.client(
            'application-autoscaling', endpoint_url='http://localhost:8000')
    else:
        resource = boto3.resource('dynamodb')
        client = boto3.client('dynamodb')
        auto_scaling_client = boto3.client('application-autoscaling')
    return resource, client, auto_scaling_client


def thread_batches(table, n_threads, items, write=False, index=None,
                   capacity=None):
    LOGGER.debug('Determining thread batches...')
    batch_size, _ = items_per_second(
        table, write=write, index=index, capacity=capacity)
    LOGGER.debug('batch_size: %s' % batch_size)
    batch_size = min(len(items), batch_size)
    LOGGER.debug('min(len(items), batch_size): %s' % batch_size)
    thread_batch_size = int(math.floor(
        batch_size / float(n_threads)))
    thread_batch_size = max(1, thread_batch_size)
    LOGGER.debug('thread_batch_size: %s' % thread_batch_size)
    n_batches = int(
        math.ceil(len(items) / float(thread_batch_size)))
    LOGGER.debug('n_batches: %s' % n_batches)
    batches = py_util.chunk(items, thread_batch_size)
    return batches, n_batches


REGIONS = {
    'china': 'cn-north-1',
    'dev': 'ap-southeast-1',
    'prod': 'us-west-2',
    'cnprod': 'cn-north-1',
    'local': 'us-west-2'
}


def batch_op(op, items, profile_name, table_name, index, capacity, n_threads,
             attr=None):
    from pathos import multiprocessing
    import functools
    from tqdm import tqdm

    ddb = LambdaDDB(profile_name=profile_name)
    table = ddb.table(table_name)
    batches, n_batches = thread_batches(table, n_threads, items, write=True,
                                        index=index, capacity=capacity)

    pool = multiprocessing.Pool(processes=n_threads)
    exception = None
    with tqdm(total=n_batches, desc='Batch') as batch_pbar:
        partial_func = functools.partial(batch_fn,
                                         op=op,
                                         table_name=table_name,
                                         profile_name=profile_name,
                                         region_name=REGIONS[profile_name],
                                         attr=attr)
        for exception in pool.imap_unordered(partial_func, batches):
            if exception:
                pool.terminate()
                raise exception
            batch_pbar.update()
    pool.close()

    return exception


# this one needed for uploader multithreading - a pickleable function
def batch_fn(batch, op, profile_name, region_name, table_name, attr=None):
    # The average time for the init phase above is only about 0.2secs,
    # but showed some variance (as low as 0.06s), so safe to leave at 1s here.

    ddb = LambdaDDB(profile_name)
    table = ddb.table(table_name)
    error = False

    for item in batch:
        try:
            if op == 'write':
                table.put_item(Item=item)
            elif op == 'delete':
                key = obj_key(item, table_name)
                table.delete_item(Key=key)
            elif op == 'update':
                key = obj_key(item, table_name)
                table.update_item(
                    Key=key,
                    UpdateExpression='set %s = :x' % attr,
                    ExpressionAttributeValues={':x': item[attr]})
            else:
                raise ValueError('Unexpected op: %r' % op)
        except Exception as e:
            LOGGER.error('Error upserting item:')
            LOGGER.error(item)
            py_util.print_exception_info()
            error = e

    # TODO: hopefully get rid of this
    time.sleep(1)

    return error


class LambdaDDB(object):

    def __init__(self, profile_name=None):
        self.resource, self.client, self.auto_scaling_client = \
            get_resources(profile_name)
        self.query_paginator = self.client.get_paginator('query')
        self.scan_paginator = self.client.get_paginator('scan')
        # repositories

    def table(self, table_name):
        return self.resource.Table(table_name)

    @staticmethod
    def pagination(paginator, args, page_size, limit=None):
        items = []
        for page in paginator.paginate(**args):
            if page['Count'] > 0:
                items += page['Items']
            if limit:
                if len(items) >= limit:
                    items = items[:limit]
                    break
            if page['Count'] == page_size:
                LOGGER.debug('Got %s items, sleeping 1 sec...' % page['Count'])
                time.sleep(1)
        return items

    @py_util.timed_func
    def delete(self, items, table_name, index=None, capacity=None, sleep=True):
        table = self.table(table_name)
        batch_size, num_batches = items_per_second(
            table, write=True, index=index, capacity=capacity)
        for batch in py_util.chunk(items, batch_size):
            for item in batch:
                key = obj_key(item, table_name)
                table.delete_item(Key=key)
            if sleep:
                time.sleep(1)

    @staticmethod
    def drop_missing_attrs(items, attr_names, projection):
        LOGGER.debug('Dropping items without all attributes...')
        attrs = [(attr_names[a]
                  if (attr_names is not None and a in attr_names.keys())
                  else a).strip()  # take care of errant spaces
                 for a in projection.split(',')]
        LOGGER.debug('Attributes required:')
        LOGGER.debug(attrs)
        items = [i for i in items if all(a in i.keys() for a in attrs)]
        LOGGER.debug('%s items remain:' % len(items))
        return items

    @py_util.timed_func
    def query(self, table_name, condition, attr_values, projection=None,
              attr_names=None, drop_missing_attrs=True,
              index_name=None, capacity=None, reverse_order=False,
              limit=None):
        # attr_values is a dict, not object
        table = self.table(table_name)
        page_size, num_pages = items_per_second(
            table, index=index_name, capacity=capacity)
        args = {
            'TableName': table_name,
            'PaginationConfig': {'PageSize': page_size},
            'Select': 'SPECIFIC_ATTRIBUTES',
            'KeyConditionExpression': condition,
            'ExpressionAttributeValues': attr_values}
        if reverse_order:
            args['ScanIndexForward'] = False
        if projection:
            args['Select'] = 'SPECIFIC_ATTRIBUTES'
            args['ProjectionExpression'] = projection
        else:
            args['Select'] = 'ALL_ATTRIBUTES'
        if attr_names:
            args['ExpressionAttributeNames'] = attr_names
        if index_name:
            args['IndexName'] = index_name
        LOGGER.debug('Querying table %s with args:' % table_name)
        LOGGER.debug(args)
        items = self.pagination(
            self.query_paginator, args, page_size, limit=limit)
        LOGGER.debug('Query of %s returned %s items:'
                     % (table_name, len(items)))
        items = parse_items(items)
        if projection and drop_missing_attrs:
            items = self.drop_missing_attrs(items, attr_names, projection)
        LOGGER.debug(items)
        return items

    def query_one(self, table_name, condition, attr_values, projection=None,
                  attr_names=None, drop_missing_attrs=False, index_name=None,
                  error_not_found=True):
        items = self.query(
            table_name=table_name, projection=projection, condition=condition,
            attr_values=attr_values, attr_names=attr_names,
            drop_missing_attrs=drop_missing_attrs, index_name=index_name,
            capacity=2)  # capacity 2 here avoids a sleep
        if len(items) == 0:
            if error_not_found:
                LOGGER.error('No records returned!')
                raise errors.NoRecord('No records returned.')
            else:
                return None
        if len(items) > 1:
            LOGGER.error('More than one record returned!')
            raise errors.MoreThanOneRecord('More than one record returned.')
        return items[0]

    @py_util.timed_func
    def scan(self, table_name, projection=None, _filter=None, attr_names=None,
             attr_values=None, max_items=None, drop_missing_attrs=True,
             capacity=None):
        # attr_values is a dict, not object
        table = self.table(table_name)
        page_size, num_pages = items_per_second(
            table, capacity=capacity)
        pagination_config = {'PageSize': page_size}
        if max_items:
            pagination_config['MaxItems'] = max_items
        args = {'TableName': table_name,
                'PaginationConfig': pagination_config}
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
        LOGGER.debug('Scanning table %s with args:' % table_name)
        LOGGER.debug(args)
        items = self.pagination(self.scan_paginator, args, page_size)
        LOGGER.debug('Scan of %s returned %s items:'
                    % (table_name, len(items)))
        items = [parse_result(item) for item in items]
        if projection and drop_missing_attrs:
            items = self.drop_missing_attrs(items, attr_names, projection)
        LOGGER.debug(items)
        return items

    def update(self, items, table_name, attribute, index=None, capacity=None,
               sleep=True):
        # TODO: parallelization
        # NOTE: this handles a single attribute, for multiples try an upsert
        table = self.table(table_name)
        batch_size, num_batches = items_per_second(
            table, write=True, index=index, capacity=capacity)
        for batch in py_util.chunk(items, batch_size):
            for item in batch:
                key = obj_key(item, table_name)
                table.update_item(
                    Key=key,
                    UpdateExpression='set %s = :x' % attribute,
                    ExpressionAttributeValues={':x': item[attribute]})
            if sleep:
                time.sleep(1)

    def upsert(self, items, table_name, index=None, capacity=None, sleep=True):
        # TODO: parallelization
        table = self.table(table_name)
        batch_size, num_batches = items_per_second(
            table, write=True, index=index, capacity=capacity)
        for batch in py_util.chunk(items, batch_size):
            for item in batch:
                table.put_item(Item=item)
            if sleep:
                time.sleep(1)


#
# Table Saver


class TableSaver(object):

    def __init__(self, ddb, year_month_day, max_mem=5000):
        self.ddb = ddb
        self.max_mem = max_mem
        self.bin = []
        self.bin_i = 0
        self.date_folder = os.path.join(os.getcwd(), 'data', year_month_day)
        self.info_path = os.path.join(self.date_folder, 'info.json')

    def table_info(self, table_name):
        table = self.ddb.resource.Table(table_name)
        item_count = table.item_count
        if item_count == 0:
            print('This table has no items.')
            return
        total_size = table.table_size_bytes / float(1000)
        avg_size = total_size / float(item_count)
        read_provision = table.provisioned_throughput['ReadCapacityUnits'] * 4
        write_provision = table.provisioned_throughput['WriteCapacityUnits'] * 4
        items_per_sec, num_pages = records_per_second(table)
        bin_size = int(math.floor(self.max_mem / avg_size))
        num_bins = int(math.ceil(item_count / float(bin_size)))

        print('Table name:           %s' % table_name)
        print('Item count:           %s' % table.item_count)
        print('Size (kb):            %s' % total_size)
        print('Avg. item size (kb):  %s' % avg_size)
        print('Read provision (kb):  %s' % read_provision)
        print('Safe reads/sec:       %s' % items_per_sec)
        print('Num pages/full scan:  %s' % num_pages)
        print('Bin size:             %s' % bin_size)
        print('Num bins:             %s' % num_bins)

        table_info = {
            'name': table_name,
            'key_schema': table.key_schema,
            'attribute_definitions': table.attribute_definitions,
            'global_secondary_indexes': table.global_secondary_indexes,
            'local_secondary_indexes': table.local_secondary_indexes,
            'provisioned_throughput': table.provisioned_throughput,
            'item_count': table.item_count,
            'table_size_kb': total_size,
            'avg_item_size_kb': avg_size,
            'read_provision': read_provision,
            'write_provision': write_provision,
            'safe_items_per_sec': items_per_sec,
            'num_pages': num_pages,
            'max_mem': self.max_mem,
            'bin_size': bin_size,
            'num_bins': num_bins,
        }

        return table_info

    def save(self, table_name):
        print('Saving table %s...' % table_name)
        self.bin = []
        self.bin_i = 0

        table_info = self.table_info(table_name)

        pagination_args = {
            'TableName': table_name,
            'PaginationConfig': {'PageSize': table_info['safe_items_per_sec']},
            'Select': 'ALL_ATTRIBUTES'}
        pagination = self.ddb.scan_paginator.paginate(**pagination_args)

        from tqdm import tqdm
        with tqdm(total=table_info['num_pages']) as pbar:
            for page in pagination:
                if page['Count'] > 0:
                    items = page['Items']
                    self.bin += items
                    if len(self.bin) > table_info['bin_size']:
                        print('Max bin size reached, dumping pickle...')
                        self.save_bin(table_info)
                if page['Count'] == table_info['safe_items_per_sec']:
                    time.sleep(1)
                pbar.update()
            # if all pages processed and still items in the bin, write
            if len(self.bin) > 0:
                print('Pagination completed, dumping pickle...')
                self.save_bin(table_info)

    def save_bin(self, table_info):
        save_path = os.path.join(
            self.date_folder, table_info['name'], '%s.pkl' % self.bin_i)
        data = {
            'table_info': table_info,
            'bin': self.bin_i,
            'items': self.bin
        }
        pickle.dump(data, open(save_path, 'w'))
        self.bin_i += 1
        self.bin = []

    def load_bin(self, table_name, i):
        file_path = os.path.join(self.date_folder, table_name, '%s.pkl' % i)
        return pickle.load(open(file_path, 'r'))

    def load_info(self):
        return json.load(open(self.info_path, 'r'))

    def save_info(self, info):
        open(self.info_path, 'w').write(
            json.dumps(info, default=py_util.json_serial))

    def load(self, table_name):
        info = self.load_info()
        for bin_ix in range(info[table_name]['num_bins']):
            _bin = self.load_bin(table_name, bin_ix)
            for item in _bin['items']:
                yield item
