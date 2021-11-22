"""Utilities for aggregate calculations."""
from . import date_util, py_util, lambda_util, LOGGER, ddb_util
import datetime
import decimal
import time
import boto3
import functools
from pathos import multiprocessing
from tqdm import tqdm


#
# Keys


def day_key(date):
    """Map a date to a day key.

    Args:
      date: String in format "yyyy-mm-dd".

    Returns:
      timestamp: Integer timestamp at 00:00 on the date.
      readable: String like "yyyy-mm-dd".
    """
    _date = date_util.str_to_date(date)
    timestamp = date_util.timestamp_from_date(_date)
    # in case the input is wrong
    if 'T' in date:
        date = date.split('T')[0]
    return timestamp, date


def week_key(date):
    """Map a date to a week key.

    Readable form:

    Args:
      date: String in format "yyyy-mm-dd".

    Returns:
      timestamp: Integer timestamp at 00:00 on the first day of the week/
      readable: String like start_date}_end_date}, where the date looks
                like yyyy-mm-dd.
    """
    date = date_util.str_to_date(date)
    weekday = date.weekday()
    first_day = date - datetime.timedelta(days=weekday)
    last_day = date + datetime.timedelta(days=6 - weekday)
    readable = '%s_%s' % (date_util.date_to_str(first_day),
                          date_util.date_to_str(last_day))
    timestamp = date_util.timestamp_from_date(first_day)
    return timestamp, readable


def month_key(date):
    """Map a date to a month key.

    Args:
      date: String in format "yyyy-mm-dd".

    Returns:
      timestamp: Integer timestamp at 00:00 on the first day of the month.
      readable: String like year-month.
    """
    date = date_util.str_to_date(date)
    first_date = datetime.date(date.year, date.month, 1)
    timestamp = date_util.timestamp_from_date(first_date)
    readable = '%s-%s' % (date.year, py_util.pad(first_date.month))
    return timestamp, readable


def get_key(date, period):
    """Get the key for the period the date relates to.

    Args:
      date: String in format "yyyy-mm-dd".
      period: String in {'daily', 'weekly', 'monthly'}.

    Returns:
      Dictionary like {'timestamp': Integer, 'readable': String}.

    Raises:
      ValueError if period is not in the expected set.
    """
    if period == 'daily':
        return day_key(date)
    elif period == 'weekly':
        return week_key(date)
    elif period == 'monthly':
        return month_key(date)
    else:
        raise ValueError('Unexpected period %r' % period)


#
# Aggregates Dictionaries


def new_agg_dict():
    """Get a new dict for aggregate values.

    The quantities of interest do not vary over objects so this generalizes.

    Returns:
      Dictionary.
    """
    return {
        'as': 0,
        'ac': 0,
        'd': 0,
        'ad': 0,
        'p': 0
    }


def new_aggs_attr():
    """Return a aggregates dictionary for the attribute on a new record.

    Returns:
      Dictionary.
    """
    all_time = new_agg_dict()
    return {
        'daily': [],
        'weekly': [],
        'monthly': [],
        'all_time': all_time}


def new_daily(date):
    """Return a new daily aggregate record.

    Returns:
      Dictionary.
    """
    timestamp, readable = day_key(date)
    aggs = new_agg_dict()
    aggs['k'] = timestamp
    aggs['day'] = readable
    return aggs


def new_weekly(date):
    """Return a new weekly aggregate record.

    Returns:
      Dictionary.
    """
    timestamp, readable = week_key(date)
    aggs = new_agg_dict()
    aggs['k'] = timestamp
    aggs['w'] = readable
    return aggs


def new_monthly(date):
    """Return a new monthly aggregate record.

    Returns:
      Dictionary.
    """
    timestamp, readable = month_key(date)
    aggs = new_agg_dict()
    aggs['k'] = timestamp
    aggs['m'] = readable
    return aggs


def new_agg(date, period):
    """Get a new aggregate for the date and period.

    Args:
      date: String in format "yyyy-mm-dd".
      period: String in {'daily', 'weekly', 'monthly'}.

    Returns:
      Dictionary.

    Raises:
      ValueError if period not in the expected set.
    """
    if period == 'daily':
        return new_daily(date)
    elif period == 'weekly':
        return new_weekly(date)
    elif period == 'monthly':
        return new_monthly(date)
    else:
        raise ValueError('Unexpected period %r' % period)


#
# Functions on Aggregate Dictionaries


def get_aggregate(obj, date, period):
    """Get or create a day aggregate.

    Args:
      obj: Dictionary, a complete object, e.g. a student or class.
      date: String in format "yyyy-mm-dd".
      period: String in {'daily', 'weekly', 'monthly'}.

    Returns:
      Dictionary, an aggregate record for the day in question.
    """
    key, _ = get_key(date, period)
    if key not in [x['k'] for x in obj['aggregates'][period]]:
        return new_agg(date, period)
    else:
        return next(x for x in obj['aggregates'][period]
                    if x['k'] == key)


def sort_aggregates(obj, period):
    """Sort the aggregate records for a period.

    Args:
      obj: Dictionary, a complete object, e.g. a student or class.
      period: String in {'daily', 'weekly', 'monthly'}.
    """
    obj['aggregates'][period] = list(sorted(
        obj['aggregates'][period],
        key=lambda x: x['k']))


def merge(aggregate, obj, period):
    """Merge a new or updated aggregate record into the aggregates attribute.

    Args:
      aggregate: Dictionary, a period aggregate (i.e. weekly or monthly).
      obj: Dictionary, a complete object, e.g. a student or class.
      period: String in {'daily', 'weekly', 'monthly'}.
    """
    # if the aggregate already exists, remove the old one
    if aggregate['k'] in [x['k'] for x in obj['aggregates'][period]]:
        old = next(a for a in obj['aggregates'][period]
                   if a['k'] == aggregate['k'])
        obj['aggregates'][period].remove(old)
    # add the new one
    obj['aggregates'][period].append(aggregate)
    # sort the aggregates in ascending time order
    sort_aggregates(obj, period)


def update_average(n1, a1, n2, a2):
    """Update average score.

    This is not for a generic average - we are returning an int by convention.

    Args:
      n1: Integer, the number of items in average 1.
      a1: Decimal, average 1.
      n2: Integer, the number of items in average 2.
      a2: Decimal, average 2.

    Returns:
      Decimal.
    """
    return int(round((n1 * a1 + n2 * a2) / decimal.Decimal(str(n1 + n2)), 0))


def aggs_for_week(obj, date):
    start, end = date_util.week_start_end(date_util.str_to_date(date))
    return [a for a in obj['aggregates']['daily']
            if start <= a['k'] < end]


def aggs_for_month(obj, date):
    start, end = date_util.month_start_end(date_util.str_to_date(date))
    return [x for x in obj['aggregates']['daily']
            if start <= x['k'] < end]


def update_period_from_children(parent, children):
    """Updates a parent aggregate record from its children.

    Transforms the parent, returning nothing.

    Args:
      parent: Dict, parent aggregate for a specific period.
      children: List of dicts, child aggregates for the specific period.

    Raises:
      ValueError if `_type` not in the expected set.
    """
    # avg score
    sum_of_scores = sum(a['as'] * a['ac']
                        for a in children)
    activity_count = sum(a['ac'] for a in children)
    if activity_count == 0:
        parent['as'] = 0
    else:
        parent['as'] = int(round(
            sum_of_scores / float(activity_count),
            0))

    # activity count
    parent['ac'] = activity_count

    # duration
    parent['d'] = sum(a['d'] for a in children)

    # average duration
    sum_of_durations = sum(a['d'] for a in children)
    if activity_count > 0:
        parent['ad'] = int(round(
            sum_of_durations / float(activity_count),
            0))
    else:
        parent['ad'] = 0

    # progress
    children_progresses = [a['p'] for a in children
                           if 'p' in a.keys()]
    parent_progress = parent['p'] \
        if 'p' in parent.keys() \
        else 0
    parent['p'] = max(
        max(children_progresses) if len(children_progresses) > 0 else 0,
        parent_progress)


def update_obj_from_children(parent, children_daily_aggs,
                             children_all_time_progresses,
                             parent_all_time_progress):
    # avg score
    sum_of_scores = sum(a['as'] * a['ac']
                        for a in children_daily_aggs)
    activity_count = sum(a['ac'] for a in children_daily_aggs)
    if activity_count == 0:
        parent['as'] = 0
    else:
        parent['as'] = int(round(
            sum_of_scores / float(activity_count),
            0))

    # activity count
    parent['ac'] = activity_count

    # duration
    parent['d'] = sum(a['d'] for a in children_daily_aggs)

    # average duration
    sum_of_durations = sum(a['d'] for a in children_daily_aggs)
    if activity_count > 0:
        parent['ad'] = int(round(
            sum_of_durations / float(activity_count),
            0))
    else:
        parent['ad'] = 0

    # progress
    if len(children_all_time_progresses) > 0:
        children_mean = int(ddb_util.avg(
            sigma=sum(children_all_time_progresses),
            n=len(children_all_time_progresses),
            dp=0))
        parent['p'] = max(parent_all_time_progress, children_mean)
    else:
        parent['p'] = parent_all_time_progress


def get_created_date(obj):
    # on this date we switched on aggs
    switched_on = datetime.date(2018, 10, 24)
    # this is when the object was created in the DB
    obj_created = date_util.date_from_timestamp(obj['createdMS'])
    obj_created = datetime.date(obj_created.year,
                                obj_created.month,
                                obj_created.day)
    # we want to start counting from the latest of these dates
    return max(switched_on, obj_created)


def set_avg_daily_duration(agg, daily_aggs, period, created):
    # determine set of all dates in the period
    date = date_util.date_from_timestamp(agg['k'])
    date = date_util.date_time_to_date(date)
    if period == 'weekly':
        dates = date_util.days_for_week(date, to_date=False)
    elif period == 'monthly':
        dates = date_util.days_for_month(date, to_date=False)
    else:
        raise ValueError('Unexpected period %r' % period)
    # filter out dates earlier than the creation date and later than today
    dates = [d for d in dates if date_util.date_time_to_date(d) >= created]
    # filter out dates later than today
    dates = [d for d in dates
             if date_util.date_time_to_date(d) <= datetime.date.today()]
    # map those dates to daily keys
    dates = [date_util.date_to_str(d) for d in dates]
    keys = [day_key(d)[0] for d in dates]
    # determine the daily durations
    durations = [a['d']
                 for a in daily_aggs
                 if a['k'] in keys]
    if len(keys) == 0:
        agg['add'] = 0
    else:
        agg['add'] = int(round(sum(durations)
                                              / float(len(keys)), 0))


def set_all_time_avg_daily_duration(agg, monthly_aggs, created):
    total_duration = sum(a['d'] for a in monthly_aggs)
    month_keys = [a['k'] for a in monthly_aggs]  # timestamps
    first_month_key = min(month_keys)
    last_month_key = max(month_keys)
    first_date = date_util.date_from_timestamp(first_month_key)
    first_date = date_util.date_time_to_date(first_date)
    first_day_last_month = date_util.date_from_timestamp(last_month_key)
    last_date = date_util.last_day_of_month(first_day_last_month.year,
                                            first_day_last_month.month)
    first_date = max(first_date, created)
    play_dates = date_util.date_range(first_date, last_date)
    # filter out dates later than today
    play_dates = [d for d in play_dates
             if date_util.date_time_to_date(d) <= datetime.date.today()]
    n_days = len(play_dates)
    if n_days == 0:
        agg['add'] = 0
    else:
        agg['add'] = int(round(total_duration / float(n_days), 0))


def calc_weekly(obj, date):
    agg = get_aggregate(obj, date, 'weekly')
    daily_aggs = aggs_for_week(obj, date)
    update_period_from_children(agg, daily_aggs)
    created = get_created_date(obj)
    set_avg_daily_duration(agg, daily_aggs, 'weekly', created)
    merge(agg, obj, 'weekly')


def calc_monthly(obj, date):
    agg = get_aggregate(obj, date, 'monthly')
    daily_aggs = aggs_for_month(obj, date)
    update_period_from_children(agg, daily_aggs)
    created = get_created_date(obj)
    set_avg_daily_duration(agg, daily_aggs, 'monthly', created)
    merge(agg, obj, 'monthly')


def calc_all_time(obj):
    monthly_aggs = obj['aggregates']['monthly']
    all_time_aggs = obj['aggregates']['all_time']
    update_period_from_children(all_time_aggs, monthly_aggs)
    created = get_created_date(obj)
    set_all_time_avg_daily_duration(all_time_aggs, monthly_aggs, created)


def update_hierarchy(obj, daily_agg):
    date = daily_agg['day']
    calc_weekly(obj, date)
    calc_monthly(obj, date)
    calc_all_time(obj)


def has_aggregates(obj):
    """Determine if an object has an aggregates record yet.

    It should be a simple "is aggregates in keys" expression, but due to the
    existence of old-style aggregates in the US data we need this somewhat more
    involved function.

    Args:
      obj: Dictionary, a full object - e.g. a student or class.

    Returns:
      Bool.
    """
    if 'aggregates' not in obj.keys():
        LOGGER.debug('aggregates attr not in object keys -> does not have aggs.')
        return False
    if len(obj['aggregates']['daily']) == 0:
        LOGGER.debug('aggregates.daily has no records -> does not have aggs.')
        return False
    # this is the real test for an old style record (that needs overwriting)
    # the previous check is to make sure we don't get an error from indexing
    # it is fine to override in the previous case
    # that will only occur with no data
    if 'k' not in obj['aggregates']['daily'][0].keys():
        LOGGER.debug('k attr not in aggs.daily -> does not have aggs.')
        return False
    else:
        return True


def filter_old(aggs):
    """Filter out old aggregates records.

    Current convention is to keep:
    - 32 days
    - 8 weeks
    - months currently not limited

    Args:
      aggs: Dict, a full aggregates record.
    """
    # TODO: I think sorting is redundant, but doing it for security - rethink
    aggs['daily'] = list(sorted(aggs['daily'], key=lambda x: x['k']))
    aggs['weekly'] = list(sorted(aggs['weekly'], key=lambda x: x['k']))
    if len(aggs['daily']) > 32:
        aggs['daily'] = aggs['daily'][-32:]
    if len(aggs['weekly']) > 8:
        aggs['weekly'] = aggs['weekly'][-8:]


def get_daily_aggs(objs, date):
    if 'T' in date:
        date = date.split('T')[0]
    daily_aggs = py_util.flatten([x['aggregates']['daily'] for x in objs])
    # filter out old defunct records
    daily_aggs = [x for x in daily_aggs if 'day' in x.keys()]
    return [x for x in daily_aggs if x['day'] == date]


AGG_CHILDREN = {
    'classes': 'students',
    'teachers': 'classes',
    'schools': 'teachers',
    'school-groups': 'schools'
}


#
# Lambdas


def parallel_agg_stats(stats, date, profile_name, capacity, n_threads):
    ddb = ddb_util.LambdaDDB(profile_name)
    table = ddb.table('students')
    batches, n_batches = ddb_util.thread_batches(
        table=table, n_threads=n_threads, items=stats, write=True, index=None,
        capacity=capacity)

    pool = multiprocessing.Pool(processes=n_threads)
    students = []
    with tqdm(total=n_batches, desc='Batch') as batch_pbar:
        partial_func = functools.partial(agg_stats,
                                         date=date,
                                         profile_name=profile_name)
        for _students in pool.imap_unordered(partial_func, batches):
            students += _students
            batch_pbar.update()
    pool.close()

    return students


def agg_stats(stats, date, profile_name):
    ddb = ddb_util.LambdaDDB(profile_name=profile_name)
    students = []

    for stat in stats:
        student = ddb.query_one(
            table_name='students',
            condition='class_code = :cc and student_pin = :sp',
            projection='class_code,student_pin,aggregates,createdMS',
            attr_values={':cc': {'S': stat['cls']},
                         ':sp': {'S': stat['usr']}},
            error_not_found=False)

        # in case the student is, e.g., recently deleted
        if student is None:
            continue

        # check the student has an aggregate record and create if no
        if not has_aggregates(student):
            student['aggregates'] = new_aggs_attr()

        # find or create the daily aggregate
        agg = get_aggregate(student, date, 'daily')

        # avg_score update
        agg['as'] = update_average(
            n1=agg['ac'],
            a1=agg['as'],
            n2=stat['a'],
            a2=stat['s'])

        # activity_count update
        agg['ac'] += stat['a']

        # duration update
        agg['d'] += stat['d']

        # average duration update
        agg['ad'] = int(round(
            agg['d'] / float(agg['ac']), 0))

        # progress update
        prev_daily_aggs = [
            a for a in student['aggregates']['daily']
            if (a['k'] < agg['k'] and 'p' in a.keys())]
        prev_max_progress = 0
        if len(prev_daily_aggs) > 0:
            prev_max_progress = max(a['p'] for a in prev_daily_aggs)
        agg['p'] = max(prev_max_progress, stat['p'])

        # merge daily results
        merge(agg, student, 'daily')

        # send the updates up the period hierarchy
        update_hierarchy(student, agg)

        # filter old aggregate records
        filter_old(student['aggregates'])

        students.append(student)

    return students


class StudentAggregates(lambda_util.Lambda2):

    def expected_path_params(self):
        return ['date', 'consume']

    def function_logic(self, event, context):
        # retrieve date from params, no need to parse it
        date, consume = self.get_path_parameters(event)

        # get hourly stats (the basis of the hourly update)
        hourly_stats = self.ddb.scan(
            table_name='hourly-stats',
            projection='cls,usr,l,s,d,a,p',
            drop_missing_attrs=True,
            capacity=self.capacity)

        # update student aggregates with hourly stats
        students = parallel_agg_stats(
            stats=hourly_stats,
            date=date,
            profile_name=self.profile_name,
            capacity=self.capacity,
            n_threads=self.n_threads)

        # update the students in the db
        ddb_util.batch_op(
            op='update',
            items=students,
            profile_name=self.profile_name,
            table_name='students',
            index=None,
            capacity=self.capacity,
            n_threads=self.n_threads,
            attr='aggregates')

        # delete the stats (if consume flag set - this is in here for testing)
        if consume:
            ddb_util.batch_op(
                op='delete',
                items=hourly_stats,
                profile_name=self.profile_name,
                table_name='hourly-stats',
                index=None,
                capacity=self.capacity,
                n_threads=self.n_threads)

        return lambda_util.ok({'agg_result': 'dancing'})


def parallel_agg_parents(parents, _type, date, profile_name, capacity,
                         n_threads):
    ddb = ddb_util.LambdaDDB(profile_name=profile_name)
    table = ddb.table(_type)

    batches, n_batches = ddb_util.thread_batches(
        table=table, n_threads=n_threads, items=parents, write=True, index=None,
        capacity=capacity)

    pool = multiprocessing.Pool(processes=n_threads)
    to_update = []
    with tqdm(total=n_batches, desc='Batch') as batch_pbar:
        partial_func = functools.partial(agg_parents,
                                         _type=_type,
                                         date=date,
                                         profile_name=profile_name,
                                         capacity=capacity)
        for _to_update in pool.imap_unordered(partial_func, batches):
            to_update += _to_update
            batch_pbar.update()
    pool.close()

    return to_update


def agg_parents(parents, _type, date, profile_name, capacity):
    ddb = ddb_util.LambdaDDB(profile_name=profile_name)
    to_update = []

    for parent in parents:
        keys = [parent[x['attr']] for x in ddb_util.ddb_keys(_type).values()
                if x['attr'] is not None]
        LOGGER.info('Aggregating %s %s' % (_type, keys))

        # need this in two places
        all_time_progress = parent['aggregates']['all_time']['p'] \
            if 'aggregates' in parent.keys() else 0

        # check if this parent has an aggregates attribute and create if not
        if not has_aggregates(parent):
            LOGGER.debug('No prior aggregates found.')
            parent['aggregates'] = new_aggs_attr()

        # fetch the children
        child_condition, attr_values, attr_names = get_child_args(parent, _type)
        children = ddb.query(
            table_name=AGG_CHILDREN[_type],
            projection='aggregates',
            condition=child_condition,
            attr_names=attr_names,
            attr_values=attr_values,
            capacity=capacity,
            drop_missing_attrs=True)  # don't need records without aggs
        LOGGER.info('Found %s children' % len(children))

        # get the daily aggregates for the day in the call
        children_daily_aggs = get_daily_aggs(children, date)

        # if no aggs for this day, init a new one
        if len(children_daily_aggs) == 0:
            LOGGER.debug('No child activity')
            new_agg = new_daily(date)
            new_agg['p'] = all_time_progress

        # otherwise create a new agg and update it from the children
        else:
            LOGGER.debug('Children have activities')

            # find or create the daily aggregate
            new_agg = get_aggregate(parent, date, 'daily')
            LOGGER.info('Parent aggregate before update:')
            LOGGER.info(new_agg)

            # all-time progresses for children for update
            children_all_time_progresses = [
                x['aggregates']['all_time']['p'] for x in children]

            # update the agg
            update_obj_from_children(
                parent=new_agg,
                children_daily_aggs=children_daily_aggs,
                children_all_time_progresses=children_all_time_progresses,
                parent_all_time_progress=all_time_progress)
            LOGGER.info('Updated agg:')
            LOGGER.info(new_agg)

        # merge daily results
        merge(new_agg, parent, 'daily')

        # send the updates up the period hierarchy
        update_hierarchy(parent, new_agg)

        # delete old aggregate records
        filter_old(parent['aggregates'])

        # add the class to the list to be updated
        to_update.append(parent)

    return to_update


def get_child_args(parent, _type):
    attr_names = {}
    attr_values = {}
    child_type = AGG_CHILDREN[_type]
    parent_id_attr = ddb_util.PARENT_ID_ATTR[child_type]
    condition = '%s = :id' % parent_id_attr
    if child_type == 'teachers':
        attr_names['#gs'] = 'group::school'
        attr_values[':id'] = {'S': f'{parent['groupName']}::{ parent['name']}'
                                   }
    else:
        attr_values[':id'] = {'S': parent[ddb_util.ID_ATTR[_type]]}

    return condition, attr_values, attr_names


class ParentAggregates(lambda_util.Lambda2):

    def __init__(self, _type):
        super(ParentAggregates, self).__init__()
        if _type not in AGG_CHILDREN.keys():
            raise ValueError('Unexpected _type %r' % _type)
        self._type = _type

    def expected_path_params(self):
        return ['date']

    def function_logic(self, event, context):
        LOGGER.info(f'Call to parent aggregates for{self._type}' )

        # retrieve date from params (no need to parse it)
        date, = self.get_path_parameters(event)

        # retrieve all records for this type - filters out missing createdMS
        parents = self.get_parents()
        LOGGER.info(f'There are{len(parents)} {self._type} to process.' 

        # list of records to update
        to_update = parallel_agg_parents(
            parents=parents, _type=self._type, date=date,
            profile_name=self.profile_name, capacity=self.capacity,
            n_threads=self.n_threads)
        LOGGER.info('%s/%s to be updated.' % (len(to_update), len(parents)))

        # update the classes table
        ddb_util.batch_op(
            op='update', items=to_update, profile_name=self.profile_name,
            table_name=self._type, index=None, capacity=self.capacity,
            n_threads=self.n_threads, attr='aggregates')

        return lambda_util.ok({'agg_result': 'dancing'})

    def get_parents(self):
        projection, attr_names = self.get_parent_projection()
        items = self.ddb.scan(
            table_name=self._type,
            projection=projection,
            attr_names=attr_names,
            drop_missing_attrs=False,
            capacity=self.capacity)
        # we have been dropping these though they should've been cleaned by now
        return [x for x in items if 'createdMS' in x.keys()]

    def get_parent_projection(self):
        keys = [k[0] for k in ddb_util.DDB_KEYS[self._type] if k[0]]
        projection_attrs = keys + ['aggregates', 'createdMS']
        return ddb_util.get_projection(projection_attrs)


class ClassAggregates(ParentAggregates):

    def __init__(self):
        super(ClassAggregates, self).__init__(_type='classes')


class TeacherAggregates(ParentAggregates):

    def __init__(self):
        super(TeacherAggregates, self).__init__(_type='teachers')


class SchoolAggregates(ParentAggregates):

    def __init__(self):
        super(SchoolAggregates, self).__init__(_type='schools')


class SchoolGroupAggregates(ParentAggregates):

    def __init__(self):
        super(SchoolGroupAggregates, self).__init__(_type='school-groups')


def get_parent_lambda(_type):
    if _type == 'classes':
        return ClassAggregates()
    elif _type == 'teachers':
        return TeacherAggregates()
    elif _type == 'schools':
        return SchoolAggregates()
    elif _type == 'school-groups':
        return SchoolGroupAggregates()
    else:
        raise ValueError


#
# Compression Util


MAP = {
    'avg_score': 'as',
    'activity_count': 'ac',
    'duration': 'd',
    'avg_duration': 'ad',
    'avg_daily_duration': 'add',
    'progress': 'p',
    'week': 'w',
    'month': 'm',
    'key': 'k'
}


def compress(aggs):
    for period in ['daily', 'weekly', 'monthly']:
        for agg in aggs[period]:
            for old_key, new_key in MAP.items():
                if old_key in agg.keys():
                    value = agg[old_key]
                    agg.pop(old_key)
                    agg[new_key] = value
    for old_key, new_key in MAP.items():
        if old_key in aggs['all_time'].keys():
            value = aggs['all_time'][old_key]
            aggs['all_time'].pop(old_key)
            aggs['all_time'][new_key] = value
    return aggs
