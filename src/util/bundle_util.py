#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Data structures and functions for bundling."""
import os
import math
import time
import json
import boto3
import random
import decimal
import hashlib
import hashids
import zipfile
import datetime
import binascii
import functools
import numpy as np
import pandas as pd
from tqdm import tqdm
from pathos import multiprocessing
from . import py_util, ddb_util, date_util, s3_util, errors


# TODO:
#  auto-provisioning is not playing with this at the moment
#  https://stackoverflow.com/questions/44661857/dynamodb-autoscaling-still-throttles
#  appears that when we spike it still takes some time (a minute or more?) for
#  the auto-scaling to kick in.


TARGET_PROVISIONING = {
    'classes': {
        'read': 5,
        'write': 5},
    'experience': {
        'read': 250,
        'write': 150},
    'school-groups': {
        'read': 1,
        'write': 1},
    'schools': {
        'read': 1,
        'write': 1},
    'student-assignments': {
        'read': 30,
        'write': 25},
    'students': {
        'read': 30,
        'write': 25},
    'teachers': {
        'read': 1,
        'write': 1},
    'user-accounts': {
        'read': 50,
        'write': 25}}
REGIONS = {
    'china': 'cn-north-1',
    'dev': 'ap-southeast-1',
    'prod': 'us-west-2'}
SOURCE_SCHOOLS = ['品格观山悦', '品格金地西沣', '品格华著']
SCHOOL_NAME_MAPPING = dict(zip(SOURCE_SCHOOLS,
                               ['school', 'school2', 'school3']))


def map_school_name(name):
    global SCHOOL_NAME_MAPPING
    return SCHOOL_NAME_MAPPING[name.encode('utf-8')]


def multi_thread_batch_args(table_name, ddb, n_threads, items):
    global TARGET_PROVISIONING
    table = ddb.table(table_name)
    batch_size, _ = ddb_util.items_per_second(
        table, write=True, index=None,
        capacity=TARGET_PROVISIONING[table_name]['write'])
    thread_batch_size = math.floor(
        batch_size / float(n_threads))
    thread_batch_size = int(min(25, thread_batch_size))
    thread_batch_size = max(1, thread_batch_size)
    n_batches = int(
        math.ceil(len(items) / float(thread_batch_size)))
    batches = py_util.chunk(items, thread_batch_size)
    return batches, n_batches


def batch_write(batches, n_batches, n_threads, target_db, table_name):
    pool = multiprocessing.Pool(processes=n_threads)
    processed_items = []
    exception = None
    with tqdm(total=n_batches, desc='Batch') as batch_pbar:
        partial_func = functools.partial(put_batch,
                                         table_name=table_name,
                                         profile_name=target_db,
                                         region_name=REGIONS[target_db])
        for items, exception, in pool.imap_unordered(partial_func, batches):
            processed_items += items
            if exception:
                pool.terminate()
                raise exception
            batch_pbar.update()
    return processed_items, exception


# TODO: this and batch_write share too much code - make reusable
def batch_delete(batches, n_batches, n_threads, target_db, table_name):
    pool = multiprocessing.Pool(processes=n_threads)
    processed_items = []
    exception = None
    with tqdm(total=n_batches, desc='Batch') as batch_pbar:
        partial_func = functools.partial(delete_batch,
                                         table_name=table_name,
                                         profile_name=target_db,
                                         region_name=REGIONS[target_db])
        for items, exception, in pool.imap_unordered(partial_func, batches):
            processed_items += items
            if exception:
                pool.terminate()
                raise exception
            batch_pbar.update()
    return processed_items, exception


def get_adapted_test_data_file_name(source, target, source_group_name, date):
    return '%s_%s_%s_to_%s_adapted_test_data.zip' \
        % (source, source_group_name, date, target)


def get_animal_names():
    with open('data/animal_names.txt', 'r') as f:
        animal_names = f.readlines()
    return [n.strip() for n in animal_names if n != '']


def get_test_data_file_name(source, group_name, date):
    return '%s_%s_%s_test_data.zip' % (source, group_name, date)


def json_data_path():
    return os.path.join(os.getcwd(), 'src', 'temp', 'data.json')


def json_from_s3(file_name):
    # clear out any old data
    if os.path.exists(json_data_path()):
        os.remove(json_data_path())

    # download the data from s3
    boto3.setup_default_session(profile_name='dev')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('lms-bundles')
    bucket.download_file(file_name, file_name)

    # unzip the file to the temp folder, read it
    data = unzip(file_name)

    # remove the zip file and data.json files
    os.remove(file_name)
    os.remove(json_data_path())

    return data


def json_to_s3(data, file_name):
    # clear out any old data
    if os.path.exists(json_data_path()):
        os.remove(json_data_path())

    # dump the data to json and zip it up
    zip_data(data, file_name)

    # upload it to s3
    boto3.setup_default_session(profile_name='dev')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('lms-bundles')
    bucket.upload_file(file_name, file_name)

    # remove the zip (data.json remove in zip_data())
    os.remove(file_name)


# this one needed for uploader multithreading - a pickleable function
def put_batch(batch, profile_name, region_name, table_name):
    # The average time for the init phase above is only about 0.2secs,
    # but showed some variance (as low as 0.06s), so safe to leave at 1s here.
    time.sleep(1)
    session = boto3.Session(profile_name=profile_name)
    ddb = session.resource('dynamodb', region_name=region_name)
    table = ddb.Table(table_name)
    processed_items = []
    error = False
    with table.batch_writer() as batch_writer:
        for item in batch:
            try:
                batch_writer.put_item(Item=item)
                processed_items.append(item)
            except Exception as e:
                print('Error upserting item:')
                print(item)
                py_util.print_exception_info()
                error = e

    return processed_items, error


# TODO: this and put_batch share too much code - make reusable
def delete_batch(batch, profile_name, region_name, table_name):
    time.sleep(1)
    session = boto3.Session(profile_name=profile_name)
    ddb = session.resource('dynamodb', region_name=region_name)
    table = ddb.Table(table_name)
    processed_items = []
    error = False
    with table.batch_writer() as batch_writer:
        for item in batch:
            try:
                key = ddb_util.obj_key(item, table_name)
                batch_writer.delete_item(Key=key)
                processed_items.append(item)
            except Exception as e:
                print('Error deleting item:')
                print(item)
                py_util.print_exception_info()
                error = e

    return processed_items, error


def adapted_test_data_exists(source, target, source_group_name, date):
    file_name = get_adapted_test_data_file_name(
        source, target, source_group_name, date)
    return s3_util.file_exists('dev', file_name)


def test_data_exists(source, group_name, date):
    file_name = get_test_data_file_name(source, group_name, date)
    return s3_util.file_exists('dev', file_name)


def unzip(zip_file_name):
    temp_dir = os.path.join(os.getcwd(), 'src', 'temp')
    json_file_path = os.path.join(temp_dir, 'data.json')
    with zipfile.ZipFile(zip_file_name, 'r') as zipf:
        zipf.extractall(temp_dir)
    with open(json_file_path, 'r') as f:
        data = json.loads(f.read(), parse_float=decimal.Decimal)
    return data


def zip_data(data, file_name):
    with zipfile.ZipFile(file_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        with open('src/temp/data.json', 'w') as f:
            _json = json.dumps(data, cls=py_util.DecimalEncoder)
            f.write(_json)
        zipf.write('src/temp/data.json', 'data.json')
    os.remove('src/temp/data.json')


class PrintLogger(object):

    @staticmethod
    def info(x):
        tqdm.write(str(x))

    @staticmethod
    def warn(x):
        tqdm.write(str(x))


class Adapt(object):

    def __init__(self, logger=PrintLogger(),
                 school_name_mapping=map_school_name):
        self.ddb = None
        self.logger = logger
        self.school_name_mapping = school_name_mapping

    def __call__(self, source, target, source_group_name, date):
        self.target = target
        self.ddb = ddb_util.DDB(
            profile=target, throttling=ddb_util.items_per_second)

        # fetch and unzip data
        file_name = get_test_data_file_name(source, source_group_name, date)
        data = json_from_s3(file_name)

        # assign all levels to `teacher`
        self.assign_classes(data)

        # adapt the data
        self.adapt(data, date)

        # filter out empty records
        self.filter(data)

        # save adapted data to s3
        file_name = get_adapted_test_data_file_name(
            source, target, source_group_name, date)
        json_to_s3(data, file_name)

        self.logger.info('Adaptation succeeded.')

    def adapt(self, data, date):
        self.logger.info('Adapting...')

        # set random seeds for random actions
        random.seed(42)
        np.random.seed(42)

        # make sure all names are unique before mapping
        self.make_names_unique(data)

        # create pin and name mappings
        pin_map = self.get_pin_map(data)
        name_map = self.get_name_map(data)  # teachers and students
        class_name_map = self.get_class_name_map(data['classes'])

        # due to dependencies in the data there is a strict order to updating

        for group in data['school-groups']:
            # user-account - grab before update
            user_account = next(a for a in data['user-accounts']
                                if a['username'] == group['director'])
            group['name'] = 'Group'
            group['displayName'] = 'Group'
            group['phone'] = 'group'
            group['director'] = pin_map[group['director']]
            group['demo_date'] = date
            # now update user-account
            user_account['username'] = group['director']
            user_account['phone'] = group['phone']
            user_account['displayName'] = group['displayName']
            user_account['schoolGroupName'] = 'Group'

        for school in data['schools']:
            # user-account - grab before update
            user_account = next(a for a in data['user-accounts']
                                if a['username'] == school['director'])
            school['groupName'] = 'Group'
            school['name'] = str.title(self.school_name_mapping(school['name']))
            school['displayName'] = str.title(school['name'])
            school['director'] = str.title(school['name'])
            school['phone'] = school['name'].lower()
            school['demo_date'] = date
            # now update user-account
            user_account['username'] = school['director']
            user_account['phone'] = school['phone']
            user_account['displayName'] = school['name']
            user_account['schoolGroupName'] = 'Group'
            user_account['schoolName'] = school['name']

        for teacher in data['teachers']:
            # teacher's user-account - grab it before the update
            user_account = next(a for a in data['user-accounts']
                                if a['username'] == teacher['username'])
            school_name = str.title(
                self.school_name_mapping(
                    teacher['schoolName']))
            teacher['username'] = pin_map[teacher['username']]
            teacher['displayName'] = name_map[teacher['displayName']]
            teacher['group::school'] = '%s::%s' \
                % ('Group', school_name)
            teacher['groupName'] = 'Group'
            teacher['schoolName'] = school_name
            teacher['phone'] = teacher['displayName'].lower()
            # now update the user account
            user_account['username'] = teacher['username']
            user_account['phone'] = teacher['phone']
            user_account['displayName'] = teacher['displayName']
            user_account['schoolGroupName'] = 'Group'
            user_account['schoolName'] = school_name

        for _class in data['classes']:
            school_name = str.title(
                self.school_name_mapping(
                    _class['schoolName']))
            _class['code'] = pin_map[_class['code']]
            _class['username'] = pin_map[_class['username']]  # broken?
            _class['name'] = class_name_map[_class['name']]
            _class['schoolGroupName'] = 'Group'
            _class['schoolName'] = school_name
            _class['teacherName'] = next(
                t for t in data['teachers']
                if t['username'] == _class['username'])['displayName']

        for exp in data['experience']:
            class_code, loc = exp['cls_loc'].split('_')
            exp['cls_loc'] = '%s_%s' % (pin_map[class_code], loc)
            exp['usr'] = pin_map[exp['usr']]

        for student in data['students']:
            # student's user account - grab it before it updates
            user_account = next(a for a in data['user-accounts']
                                if a['username'] == student['student_pin'])
            school_name = str.title(
                self.school_name_mapping(
                    student['schoolName']))
            student['student_pin'] = pin_map[student['student_pin']]
            student['username'] = student['student_pin']
            student['class_code'] = pin_map[student['class_code']]
            student['teacher'] = pin_map[student['teacher']]
            student['name'] = name_map[student['name']]
            student['schoolGroupName'] = 'Group'
            student['schoolName'] = school_name
            student['phone'] = student['name'].lower()
            # now update the user account
            user_account['username'] = student['student_pin']
            user_account['phone'] = student['phone']
            user_account['displayName'] = student['name']
            user_account['schoolGroupName'] = 'Group'
            user_account['schoolName'] = school_name

        for ass in data['student-assignments']:
            ass['class_code'] = pin_map[ass['class_code']]
            ass['student_pin'] = pin_map[ass['student_pin']]

        for acc in data['user-accounts']:
            # ATT TIM! # can't blank this in dynamoDB but can have it not exist.
            # identityId will be created on successful login
            if 'identityId' in acc.keys():
                acc.pop('identityId', None)

            # ATT TIM! # My attempt at matching the same pwd generation logic
            # from node. Node and python seem to share very similar libs in this
            # area, hope it works.
            # SALT:  crypto.randomBytes(64).toString('Base64');
            # HASH:  crypto.pbkdf2Sync(password, salt, 1000, 64, 'sha512')
            #            .toString('hex');
            salt = '000000'  # just for demo accounts, hardcoded.
            pwd = '123456'  # just for demo accounts, hardcoded.
            k = hashlib.pbkdf2_hmac('sha512', pwd.encode('utf-8'), salt, 1000,
                                    64)
            acc['hash'] = binascii.hexlify(k).decode('utf-8')
            acc['salt'] = salt

    @staticmethod
    def assign_classes(data):
        teacher = next(t for t in data['teachers']
                       if t['username'] == 'ZRBBOZ')
        p_teacher = next(t for t in data['teachers']
                         if t['username'] == 'QERLDZ')
        k_teacher = next(t for t in data['teachers']
                         if t['username'] == 'ZKLEEQ')

        n_classes_to_transfer = [c for c in data['classes']
                                 if c['username'] == 'ZRBBOZ'
                                 and c['code'] != 'ZJOKLQ']  # that one's kept
        n_classes_to_p_teacher = n_classes_to_transfer[0:2]
        n_classes_to_k_teacher = n_classes_to_transfer[2:]
        p_class_to_teacher = next(c for c in data['classes']
                                  if c['code'] == 'ZRKJOQ')  # any one OK
        k_class_to_teacher = next(c for c in data['classes']
                                  if c['code'] == 'ZPDLNZ')  # only one has Ss

        def transfer_class(_class, new_teacher):
            _class['username'] = new_teacher['username']
            _class['teacherName'] = new_teacher['displayName']
            for student in [s for s in data['students']
                            if s['class_code'] == _class['code']]:
                student['teacher'] = new_teacher['username']

        for _class in n_classes_to_p_teacher:
            transfer_class(_class, p_teacher)
        for _class in n_classes_to_k_teacher:
            transfer_class(_class, k_teacher)
        transfer_class(p_class_to_teacher, teacher)
        transfer_class(k_class_to_teacher, teacher)

    @staticmethod
    def filter(data):
        # NOTE: this needs to go in this order due to heirarchical dependencies
        # i.e. once we filter students, some classes may be empty. Same for
        # classes and teachers.

        # filter out students without experience
        data['students'] = [s for s in data['students']
                            if any(e['usr'] == s['student_pin']
                                   for e in data['experience'])]

        # filter out classes without students
        data['classes'] = [c for c in data['classes']
                           if any(s['class_code'] == c['code']
                                  for s in data['students'])]

        # filter out teachers with no classes
        data['teachers'] = [t for t in data['teachers']
                            if any(c['username'] == t['username']
                                   for c in data['classes'])]

    @staticmethod
    def get_class_name_map(classes):
        class_names = [c['name'] for c in classes]
        animal_names = get_animal_names()
        new_class_names = random.sample(animal_names, len(class_names))
        _map = dict(zip(class_names, new_class_names))
        n_class = next(c for c in classes if c['code'] == 'ZJOKLQ')
        p_class = next(c for c in classes if c['code'] == 'ZRKJOQ')
        k_class = next(c for c in classes if c['code'] == 'ZPDLNZ')
        _map[n_class['name']] = 'Level 1 Class'
        _map[p_class['name']] = 'Level 2 Class'
        _map[k_class['name']] = 'Level 3 Class'
        return _map

    def get_name_map(self, data):
        self.logger.info('\tGetting name map...')

        # baby names file downloaded from:
        # https://github.com/hadley/data-baby-names/blob/master/baby-names.csv

        # get set of names for mapping
        student_names = [s['name'] for s in data['students']]
        teacher_names = [t['displayName'] for t in data['teachers']]
        old_names = student_names + teacher_names
        n_names = len(old_names)
        self.logger.info('\t\tWe have %s names to map' % n_names)

        # determine already taken names in the target database
        self.logger.info('\t\tDetermining names already in ddb...')
        existing = self.ddb.scan(
            table_name='user-accounts',
            projection='phone',
            capacity=TARGET_PROVISIONING['user-accounts']['read'])
        existing = [x['phone'] for x in existing]

        # randomly select names and produce the mapping
        self.logger.info('\t\tRandomly selecting new names for mapping...')
        df = pd.read_csv(os.path.join(os.getcwd(), 'data', 'baby-names.csv'))
        new_names = []
        year = 2008
        remaining = n_names
        while remaining > 0:
            df = df[df['year'] == year]
            boys = df[df['sex'] == 'boy'].iloc[0:n_names*2]
            girls = df[df['sex'] == 'girl'].iloc[0:n_names*2]
            df = pd.concat([boys, girls])
            candidates = [x for x in list(df['name'].unique())
                          if x not in existing]
            take = max(len(candidates), remaining)
            new_names += random.sample(candidates, take)
            year -= 1
            remaining = n_names - len(new_names)
        self.logger.info('\t\tSuccess.')

        name_map = dict(zip(old_names, new_names))

        # define teacher, parent, and student manually
        teacher = next(t for t in data['teachers']
                       if t['username'] == 'ZRBBOZ')
        parent = next(s for s in data['students']
                      if s['student_pin'] == 'QNGGEZ')
        student = next(s for s in data['students']
                       if s['student_pin'] == 'ZPDMGZ')
        name_map[teacher['displayName']] = 'Teacher'
        name_map[parent['name']] = 'Parent'
        name_map[student['name']] = 'Student'

        return name_map

    def get_pin_map(self, data):
        self.logger.info('\tGetting pin map...')

        # determine how many new pins we need
        n_pins = len(data['classes']) + len(data['school-groups']) \
                 + len(data['schools']) + len(data['students']) \
                 + len(data['teachers'])
        self.logger.info('\t\tWe need %s pins' % n_pins)

        # one shot update and read to the pin counter
        self.logger.info('\t\tUpdating and fetching counter...')
        last_count = self.ddb.client.update_item(
            TableName='class-code-counters',
            Key={'name': {'S': 'default'}},
            UpdateExpression='SET #counter = #counter + :i',
            ExpressionAttributeValues={':i': {'N': str(n_pins)}},
            ExpressionAttributeNames={'#counter': 'counter'},
            ReturnValues='UPDATED_OLD')
        last_count = int(last_count['Attributes']['counter']['N'])
        self.logger.info('\t\tLast count was %s' % last_count)

        # generate new pins
        self.logger.info('\t\tGenerating new pins...')
        pin_generator = hashids.Hashids(
            min_length=6, salt='LMS', alphabet='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
        seeds = range(last_count + 1, last_count + n_pins + 1)
        pins = (pin_generator.encode(s) for s in seeds)

        # create pin mapping
        self.logger.info('\t\tCreating pin mapping...')
        pin_map = dict()
        for _class in data['classes']:
            pin_map[_class['code']] = next(pins)
        for group in data['school-groups']:
            pin_map[group['director']] = next(pins)
        for school in data['schools']:
            pin_map[school['director']] = next(pins)
        for student in data['students']:
            pin_map[student['student_pin']] = next(pins)
        for teacher in data['teachers']:
            pin_map[teacher['username']] = next(pins)
        self.logger.info('\t\tSuccess.')

        return pin_map

    @staticmethod
    def make_names_unique(data):
        name_set = {}

        def make_unique(items, name_attr):
            for item in items:
                if item[name_attr] in name_set.keys():
                    name_set[item[name_attr]] += 1
                    item[name_attr] = item[name_attr] \
                                      + str(name_set[item[name_attr]] - 1)
                else:
                    name_set[item[name_attr]] = 1

        make_unique(data['teachers'], 'displayName')
        make_unique(data['students'], 'name')


class Fetch(object):

    def __init__(self, logger=PrintLogger(),
                 target_provisioning=None):
        self.source = None
        self.ddb = None
        self.logger = logger
        self.target_provisioning = target_provisioning if target_provisioning \
                                   else TARGET_PROVISIONING

    def __call__(self, source, group_name, school_names, date,
                 error_missing=True):
        self.source = source
        self.ddb = ddb_util.DDB(
            profile=source,
            throttling=ddb_util.items_per_second)
        data = self.get_data(date, group_name, school_names, error_missing)
        file_name = get_test_data_file_name(source, group_name, date)
        json_to_s3(data, file_name)

    def get_user_account(self, user_accounts, username, error_missing):
        user_account = self.ddb.query_one(
            table_name='user-accounts',
            condition='username = :un',
            attr_values={':un': {'S': username}},
            error_not_found=error_missing)
        if not user_account and error_missing:
            raise errors.NoRecord('No user-account for %s' % username)
        if user_account:
            user_accounts.append(user_account)

    def get_data(self, date, group_name, school_names, error_missing):
        start = time.time()

        # determine set of schools to handle
        if not school_names:
            school_names = self.ddb.query(
                table_name='schools',
                projection='#name',
                condition='groupName = :gn',
                attr_names={'#name': 'name'},
                attr_values={':gn': {'S': group_name}})
            school_names = [x['name'] for x in school_names]
            assert isinstance(school_names[0], str)

        # lists to hold the data as we query
        school_groups = []
        schools = []
        teachers = []
        classes = []
        students = []
        user_accounts = []
        experience = []
        student_assignments = []

        # keep track of metadata
        meta_data = {
            'type': 'test_data',
            'source': self.source,
            'date': date,
            'group_name': group_name,
            'school_names': school_names}

        #
        # Fetch the data

        self.logger.info('Getting data...')

        # school-group
        school_group = self.ddb.query_one(
            table_name='school-groups',
            condition='#name = :n',
            attr_values={':n': {'S': group_name}},
            attr_names={'#name': 'name'})
        self.get_user_account(
            user_accounts, school_group['director'], error_missing)
        school_groups.append(school_group)

        # schools
        with tqdm(total=len(school_names), desc='School') as xx_pbar:
            for school_name in school_names:
                school = self.ddb.query_one(
                    table_name='schools',
                    condition='groupName = :gn and #name = :n',
                    attr_values={':gn': {'S': group_name},
                                 ':n': {'S': school_name}},
                    attr_names={'#name': 'name'})
                xx_pbar.set_description('School:  %s' % school['director'])
                self.get_user_account(
                    user_accounts, school['director'], error_missing)
                schools.append(school)

                # teachers
                school_teachers = self.ddb.query(
                    table_name='teachers',
                    condition='#gs = :gs',
                    attr_names={'#gs': 'group::school'},
                    attr_values={':gs': {'S': '%s::%s' % (group_name,
                                                          school_name)}},
                    capacity=self.target_provisioning['teachers']
                                                              ['read'])
                teachers += school_teachers
                with tqdm(total=len(school_teachers),
                          desc='Teacher') as tt_pbar:
                    for teacher in school_teachers:
                        tt_pbar.set_description('Teacher: %s'
                                                % teacher['username'])
                        self.get_user_account(
                            user_accounts, teacher['username'],
                            error_missing)

                        # classes
                        teacher_classes = self.ddb.query(
                            table_name='classes',
                            condition='username = :un',
                            attr_values={':un': {'S': teacher['username']}},
                            capacity=self.target_provisioning
                            ['classes']['read'])
                        classes += teacher_classes
                        with tqdm(total=len(teacher_classes),
                                  desc='Class') as cc_pbar:
                            for _class in teacher_classes:
                                cc_pbar.set_description('Class:   %s'
                                                        % _class['code'])

                                # student-assignments
                                student_assignments += self.ddb.query(
                                    table_name='student-assignments',
                                    condition='class_code = :cc',
                                    attr_values={':cc': {'S': _class['code']}},
                                    capacity=self.target_provisioning
                                    ['student-assignments']['read'])

                                # students
                                class_students = self.ddb.query(
                                    table_name='students',
                                    condition='class_code = :cc',
                                    attr_values={':cc': {'S': _class['code']}},
                                    capacity=self.target_provisioning
                                    ['students']['read'])
                                students += class_students
                                with tqdm(total=len(class_students),
                                          desc='Student') as ss_pbar:
                                    for student in class_students:
                                        ss_pbar.set_description(
                                            'Student: %s'
                                            % student['student_pin'])
                                        self.get_user_account(
                                            user_accounts,
                                            student['student_pin'],
                                            error_missing)

                                        # experience
                                        student_experience = self.ddb.query(
                                            table_name='experience',
                                            index_name='usr-date-index',
                                            condition='usr = :usr',
                                            attr_values={
                                                ':usr':
                                                    {'S': student['student_pin']
                                                     }},
                                            capacity=
                                            self.target_provisioning
                                            ['experience']['read'])
                                        experience += student_experience
                                        ss_pbar.update()
                                cc_pbar.update()
                        tt_pbar.update()
                xx_pbar.update()

        self.logger.info('Filtering...')

        # filter out teachers without classes
        teachers = [t for t in teachers
                    if any(c['username'] == t['username']
                           for c in classes)]

        # filter out classes without students (dunno if there are any)
        classes = [c for c in classes
                   if any(s['class_code'] == c['code']
                          for s in students)]

        # filter out experience records for which we don't have the class
        # this might occur, e.g., when a student changes class
        class_codes = set([c['code'] for c in classes])
        experience = [x for x in experience
                      if x['cls_loc'].split('_')[0] in class_codes]

        # filter out student-assignments missing student_pins on the same
        # reasoning
        student_pins = set([s['student_pin'] for s in students])
        student_assignments = [a for a in student_assignments
                               if a['student_pin'] in student_pins]

        time_taken = time.time() - start
        self.logger.info('\tAll data downloaded in %ss' % time_taken)

        return {
            'meta-data': meta_data,
            'classes': classes,
            'experience': experience,
            'school-groups': school_groups,
            'schools': schools,
            'student-assignments': student_assignments,
            'students': students,
            'teachers': teachers,
            'user-accounts': user_accounts}


class ProvisioningManager(object):

    def __init__(self, logger=PrintLogger()):
        self.logger = logger

    def get_baseline_provisioning(self, profile_name):
        file_name = self.get_provisioning_file_name(profile_name)
        return json_from_s3(file_name)

    @staticmethod
    def get_provisioning_file_name(profile_name):
        return '%s_baseline_provisioning.zip' % profile_name

    def increase(self, profile_name, targets=None):
        global TARGET_PROVISIONING
        if not targets:
            targets = TARGET_PROVISIONING
        self.save_current_provisioning(profile_name)
        self.update_provisioning(profile_name, targets)

    @staticmethod
    def parse_targets(targets):
        return {table_name: {'read': prov['read']['base'],
                             'write': prov['write']['base']}
                for table_name, prov in targets.items()}

    def raise_provisioning(self, profile_name):
        global TARGET_PROVISIONING
        ddb = ddb_util.DDB(profile=profile_name, logger=self.logger)
        for table_name in TARGET_PROVISIONING.keys():
            for action in ['read', 'write']:
                target = TARGET_PROVISIONING[table_name][action]
                read = action == 'read'
                _min, _ = ddb.get_auto_scaling(
                    table_name, read=read)
                if _min is not None:
                    ddb.update_auto_scaling(
                        table_name=table_name, _min=target, _max=target + 1)
                else:
                    ddb.update_provisioning(
                        table_name=table_name, **{action: target})
                for index_name in ddb.get_gsis(table_name):
                    _min, _ = ddb.get_auto_scaling(
                        table_name=table_name,
                        index_name=index_name,
                        read=read)
                    if _min is not None:
                        ddb.update_auto_scaling(
                            table_name=table_name, _min=target, _max=target + 1)
                    else:
                        ddb.update_provisioning(
                            table_name=table_name,
                            index=index_name,
                            **{action: target})

    def reset(self, profile_name):
        print('Resetting provisioning for %s...' % profile_name)
        targets = self.get_baseline_provisioning(profile_name)
        targets = self.parse_targets(targets)
        self.update_provisioning(profile_name, targets)

    def save_current_provisioning(self, profile_name):
        global TARGET_PROVISIONING
        ddb = ddb_util.DDB(
            profile=profile_name,
            logger=self.logger,
            throttling=ddb_util.items_per_second)
        provisionings = {}
        for table_name in TARGET_PROVISIONING.keys():
            provisionings[table_name] = ddb.get_provisioning(table_name)
        file_name = self.get_provisioning_file_name(profile_name)
        json_to_s3(provisionings, file_name)

    def update_provisioning(self, profile_name, targets):
        ddb = ddb_util.DDB(profile=profile_name, logger=self.logger)
        with tqdm(total=len(targets), desc='Table') as table_pbar:
            for table_name in targets.keys():
                self.logger.info('Table %s' % table_name)
                table_pbar.set_description(table_name)
                read_target = targets[table_name]['read']
                write_target = targets[table_name]['write']
                ddb.update_provisioning(
                    table_name=table_name, read=read_target)
                ddb.update_provisioning(
                    table_name=table_name, write=write_target)
                gsis = ddb.get_gsis(table_name)
                for index_name in gsis:
                    self.logger.info('\tIndex %s' % index_name)
                    ddb.update_provisioning(
                        table_name=table_name,
                        index=index_name,
                        read=read_target)
                    ddb.update_provisioning(
                        table_name=table_name,
                        index=index_name,
                        write=write_target)
                table_pbar.update()


class Upload(object):

    def __init__(self, logger=PrintLogger(), n_threads=4):
        self.ddb = None
        self.processed = {}
        self.n_threads = n_threads
        self.logger = logger

    def __call__(self, source, target, group_name, date):
        self.ddb = ddb_util.DDB(
            profile=target, throttling=ddb_util.items_per_second)

        # integrity check
        if not adapted_test_data_exists(source, target, group_name, date):
            raise ValueError('Adapt the data for this date first.')

        # fetch the adapted data from s3
        file_name = get_adapted_test_data_file_name(
            source, target, group_name, date)
        data = json_from_s3(file_name)

        # determine table batch sizes
        batch_params = {}
        for table_name in TARGET_PROVISIONING.keys():
            table = self.ddb.table(table_name)
            index = 'usr-date-index' if table_name == 'experience' else None
            # get batch size based on provisioning
            batch_size, _ = ddb_util.items_per_second(
                table, write=True, index=index,
                capacity=TARGET_PROVISIONING[table_name]['write'])

            # determine batch size based on multi-threading
            thread_batch_size = math.floor(
                batch_size / float(self.n_threads))
            thread_batch_size = int(min(25, thread_batch_size))
            thread_batch_size = max(1, thread_batch_size)

            # determine number of batches
            table_data = data[table_name]
            n_batches = int(
                math.ceil(len(table_data) / float(thread_batch_size)))

            # report params
            print('Upload batch params for table %s:' % table_name)
            print('\tItem count:               %s' % len(data[table_name]))
            print('\tProvisioned batch size:   %s' % batch_size)
            print('\tNumber of threads to use: %s' % self.n_threads)
            print('\tThread batch size:        %s' % thread_batch_size)
            print('\tNumber of batches:        %s' % n_batches)
            batch_params[table_name] = {
                'thread_batch_size': thread_batch_size,
                'n_batches': n_batches}

        # upload
        self.logger.info('Uploading...')
        start = time.time()
        with tqdm(total=len(TARGET_PROVISIONING), desc='Table') as pbar:
            for table_name in TARGET_PROVISIONING.keys():
                pbar.set_description(table_name)
                batch_size = batch_params[table_name]['thread_batch_size']
                n_batches = batch_params[table_name]['n_batches']
                batches = py_util.chunk(data[table_name], batch_size)
                processed_items, exception = batch_write(
                    batches=batches,
                    n_batches=n_batches,
                    n_threads=self.n_threads,
                    target_db=target,
                    table_name=table_name)
                self.processed[table_name] = processed_items
                if exception:
                    self.rollback()
                    raise exception
                pbar.update()

        self.logger.info('Records pushed in %ss' % (time.time() - start))

    def rollback(self):
        self.logger.info('Error encountered - rolling back...')
        for table_name, items in self.processed.items():
            self.logger.info('\t%s...' % table_name)
            index = 'usr-date-index' if table_name == 'experience' else None
            self.ddb.delete_items(table_name, items, index=index)
        self.logger.info('Success.')


class Zap(object):

    def __init__(self, logger=PrintLogger(), n_threads=6):
        self.logger = logger
        self.n_threads = n_threads

    def __call__(self, profile_name, group_name, date=None, table_name=None):
        global TARGET_PROVISIONING

        # require variables
        if not date:
            date = date_util.date_to_str(datetime.date.today())
        ddb = ddb_util.DDB(
            profile=profile_name, throttling=ddb_util.items_per_second)
        fetch = Fetch()

        # log call info
        self.logger.info('Deleting group %s from %s...'
                         % (group_name, profile_name))

        # obtain data to delete
        if not test_data_exists(profile_name, group_name, date):
            fetch(
                source=profile_name,
                group_name=group_name,
                school_names=None,
                date=date,
                error_missing=False)
        else:
            self.logger.info('Test data already on s3.')
        file_name = get_test_data_file_name(profile_name, group_name, date)
        data = json_from_s3(file_name)

        # determine table set to zap
        table_names = [table_name] if table_name else TARGET_PROVISIONING.keys()

        # delete data
        with tqdm(total=len(TARGET_PROVISIONING), desc='Table') as pbar:
            for table_name in table_names:
                pbar.set_description('Table (%s)' % table_name)
                # TODO: that's not right - total table size, not item size?
                batches, n_batches = multi_thread_batch_args(
                    table_name=table_name,
                    ddb=ddb,
                    n_threads=self.n_threads,
                    items=data[table_name])
                processed_items, exception = batch_delete(
                    batches=batches,
                    n_batches=n_batches,
                    n_threads=self.n_threads,
                    target_db=profile_name,
                    table_name=table_name)
                if exception:
                    raise exception
                pbar.update()

        # dancin'
        self.logger.info('Success.')
