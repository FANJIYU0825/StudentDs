"""Lambda to create a demo teacher.

We receive a username and password and create a copy of class QBDRPE, as already
grabbed on April 18th 2019, such that it has only completed all of some early
modules, being a good demo class. The json file with this data is packaged with
this lambda.
"""
import json
import random
import decimal
import hashids
import hashlib
import binascii
from util import lambda_util, LOGGER


class Adapt(object):

    def __init__(self, ddb):
        self.ddb = ddb

    def __call__(self, data, username, password):
        # set random seed for random actions
        random.seed(42)

        # make sure all names are unique before mapping
        self.make_names_unique(data)

        # create pin and name mappings
        pin_map = self.get_pin_map(data)
        name_map = self.get_name_map(data)  # teachers and students
        class_name_map = {data['classes'][0]['name']: 'Level 3 Class'}
        school_name = 'School'
        group_name = 'Group'

        for teacher in data['teachers']:
            # teacher's user-account - grab it before the update
            user_account = next(a for a in data['user-accounts']
                                if a['username'] == teacher['username'])
            teacher['username'] = pin_map[teacher['username']]
            teacher['displayName'] = username
            teacher['group::school'] = '%s::%s' \
                % (group_name, school_name)
            teacher['groupName'] = group_name
            teacher['schoolName'] = school_name
            teacher['phone'] = username
            teacher['demo'] = True
            # now update the user account
            user_account['username'] = teacher['username']
            user_account['phone'] = teacher['phone']
            user_account['displayName'] = teacher['displayName']
            user_account['schoolGroupName'] = group_name
            user_account['schoolName'] = school_name

        for _class in data['classes']:
            _class['code'] = pin_map[_class['code']]
            _class['username'] = pin_map[_class['username']]  # broken?
            _class['name'] = class_name_map[_class['name']]
            _class['schoolGroupName'] = group_name
            _class['schoolName'] = school_name
            _class['teacherName'] = data['teachers'][0]['displayName']
            _class['demo'] = True

        for exp in data['experience']:
            class_code, loc = exp['cls_loc'].split('_')
            exp['cls_loc'] = '%s_%s' % (pin_map[class_code], loc)
            exp['usr'] = pin_map[exp['usr']]
            exp['demo'] = True

        for student in data['students']:
            # student's user account - grab it before it updates
            user_account = next(a for a in data['user-accounts']
                                if a['username'] == student['student_pin'])
            student['student_pin'] = pin_map[student['student_pin']]
            student['username'] = student['student_pin']
            student['class_code'] = pin_map[student['class_code']]
            student['teacher'] = pin_map[student['teacher']]
            student['name'] = name_map[student['name']]
            student['schoolGroupName'] = group_name
            student['schoolName'] = school_name
            student['phone'] = student['name'].lower()
            student['demo'] = True
            # now update the user account
            user_account['username'] = student['student_pin']
            user_account['phone'] = student['phone']
            user_account['displayName'] = student['name']
            user_account['schoolGroupName'] = group_name
            user_account['schoolName'] = school_name

        # TODO: move this hack out into fetch part
        data['student-assignments'] = [a for a in data['student-assignments']
                                       if a['student_pin'] in pin_map.keys()]

        for ass in data['student-assignments']:
            ass['class_code'] = pin_map[ass['class_code']]
            ass['student_pin'] = pin_map[ass['student_pin']]
            ass['demo'] = True

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
            pwd = password  # just for demo accounts, hardcoded.
            k = hashlib.pbkdf2_hmac(
                'sha512', pwd.encode('utf-8'), salt, 1000, 64)
            acc['hash'] = binascii.hexlify(k).decode('utf-8')
            acc['salt'] = salt
            acc['demo'] = True

        self.filter(data)

        return data

    @staticmethod
    def filter(data):
        # filter out students without experience
        data['students'] = [s for s in data['students']
                            if any(e['usr'] == s['student_pin']
                                   for e in data['experience'])]

    def get_name_map(self, data):
        # baby names file downloaded from:
        # https://github.com/hadley/data-baby-names/blob/master/baby-names.csv

        # get set of names for mapping
        old_names = [s['name'] for s in data['students']]
        n_names = len(old_names)

        # determine already taken names in the target database
        existing = self.ddb.scan(
            table_name='user-accounts',
            projection='phone')
        existing = [x['phone'] for x in existing]

        # randomly select names and produce the mapping
        with open('baby-names.csv') as f:
            lines = f.readlines()
        name_set = {}
        for ix, line in enumerate(lines):
            if ix == 0:
                continue
            year, name, _, sex = line.strip().split(',')
            year = int(year)
            name = name.replace('"', '')
            sex = sex.replace('"', '')
            if year not in name_set.keys():
                name_set[year] = {}
            if sex not in name_set[year].keys():
                name_set[year][sex] = []
            if name not in name_set[year][sex]:
                name_set[year][sex].append(name)
        new_names = []
        year = 2008
        remaining = n_names
        while remaining > 0:
            current_names = name_set[year]['boy'] + name_set[year]['girl']
            candidates = [x for x in current_names
                          if x not in existing]
            take = max(len(candidates), remaining)
            new_names += random.sample(candidates, take)
            year -= 1
            remaining = n_names - len(new_names)
        name_map = dict(zip(old_names, new_names))

        return name_map

    def get_pin_map(self, data):
        # determine how many new pins we need
        n_pins = len(data['classes']) + len(data['students']) \
                 + len(data['teachers'])

        # one shot update and read to the pin counter
        last_count = self.ddb.client.update_item(
            TableName='class-code-counters',
            Key={'name': {'S': 'default'}},
            UpdateExpression='SET #counter = #counter + :i',
            ExpressionAttributeValues={':i': {'N': str(n_pins)}},
            ExpressionAttributeNames={'#counter': 'counter'},
            ReturnValues='UPDATED_OLD')
        last_count = int(last_count['Attributes']['counter']['N'])

        # generate new pins
        pin_generator = hashids.Hashids(
            min_length=6, salt='LMS', alphabet='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
        seeds = range(last_count + 1, last_count + n_pins + 1)
        pins = (pin_generator.encode(s) for s in seeds)

        # create pin mapping
        pin_map = dict()
        for _class in data['classes']:
            pin_map[_class['code']] = next(pins)
        for student in data['students']:
            pin_map[student['student_pin']] = next(pins)
        for teacher in data['teachers']:
            pin_map[teacher['username']] = next(pins)

        return pin_map

    @staticmethod
    def make_names_unique(data):
        """This makes the names on the records unique, prior to mapping."""
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


class CreateDemoTeacher(lambda_util.Lambda2):

    def expected_path_params(self):
        return ['username', 'password']

    def function_logic(self, event, context):
        username, password = self.get_path_parameters(event)

        # fetch
        LOGGER.info('Loading raw data...')
        with open('jakes_data_20190418.json') as f:
            data = json.loads(
                f.read(), parse_float=lambda x: decimal.Decimal(str(x)))

        # adapt
        LOGGER.info('Adapting data...')
        adapt = Adapt(self.ddb)
        data = adapt(data, username, password)

        # push it on in there
        LOGGER.info('Pushing data...')
        for table_name in data.keys():
            self.ddb.upsert(data[table_name], table_name, sleep=False)

        return lambda_util.ok({'result': 'dancing'})


def handle(event, context):
    _lambda = CreateDemoTeacher()
    return _lambda(event, context)
