"""For aggregating teacher data."""
from util import lambda_util


class CalcSchoolStats(lambda_util.Lambda2):

    def function_logic(self, event, context):
        schools = self.ddb.scan(
            table_name='schools',
            projection='groupName,#name',
            attr_names={'#name': 'name'}
        )

        table = self.ddb.table('schools')

        for school in schools:
            stats = {
                'num_teachers': 0,
                'num_classes': 0,
                'num_students': 0,
                'levels': {
                    'B': {
                        'num_students': 0,
                        'num_classes': 0
                    },
                    'P': {
                        'num_students': 0,
                        'num_classes': 0
                    },
                    'N': {
                        'num_students': 0,
                        'num_classes': 0
                    },
                    'K': {
                        'num_students': 0,
                        'num_classes': 0
                    }}
            }

            teachers = self.ddb.query(
                table_name='teachers',
                projection='stats',
                condition='#gs = :gs',
                attr_names={'#gs': 'group::school'},
                attr_values={':gs': {'S': '%s::%s' % (school['groupName'],
                                                      school['name'])}}
            )

            stats['num_teachers'] = len(teachers)
            stats['num_classes'] = sum(
                [t['stats']['num_classes'] for t in teachers])
            stats['num_students'] = sum(
                [t['stats']['num_students'] for t in teachers])
            for level in stats['levels'].keys():
                stats['levels'][level]['num_classes'] = sum(
                    [t['stats']['levels'][level]['num_classes']
                     for t in teachers])
                stats['levels'][level]['num_students'] = sum(
                    [t['stats']['levels'][level]['num_students']
                     for t in teachers])

            table.update_item(
                Key={'groupName': school['groupName'],
                     'name': school['name']},
                UpdateExpression='set stats = :s',
                ExpressionAttributeValues={':s': stats}
            )

        return lambda_util.ok({'calc_result': 'dancing'})


def handle(event, context):
    _lambda = CalcSchoolStats()
    return _lambda(event, context)
