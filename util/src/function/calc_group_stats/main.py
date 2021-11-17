"""For aggregating teacher data."""
from util import lambda_util, LOGGER


class CalcGroupStats(lambda_util.Lambda2):

    def function_logic(self, event, context):
        groups = self.ddb.scan(
            table_name='school-groups',
            projection='#name',
            attr_names={'#name': 'name'}
        )

        table = self.ddb.table('school-groups')

        for group in groups:
            LOGGER.info('Group name: %s' % group['name'])
            stats = {
                'num_schools': 0,
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

            schools = self.ddb.query(
                table_name='schools',
                projection='#name,stats',
                condition='groupName = :gs',
                attr_names={'#name': 'name'},
                attr_values={':gs': {'S': group['name']}}
            )

            stats['num_schools'] = len(schools)
            stats['num_teachers'] = sum(
                [s['stats']['num_teachers'] for s in schools])
            stats['num_classes'] = sum(
                [s['stats']['num_classes'] for s in schools])
            stats['num_students'] = sum(
                [s['stats']['num_students'] for s in schools])
            for level in stats['levels'].keys():
                stats['levels'][level]['num_classes'] = sum(
                    [s['stats']['levels'][level]['num_classes']
                     for s in schools])
                stats['levels'][level]['num_students'] = sum(
                    [s['stats']['levels'][level]['num_students']
                     for s in schools])

            LOGGER.info('Stats:')
            LOGGER.info(stats)

            table.update_item(
                Key={'name': group['name']},
                UpdateExpression='set stats = :s',
                ExpressionAttributeValues={':s': stats}
            )

        return lambda_util.ok({'calc_result': 'dancing'})


def handle(event, context):
    _lambda = CalcGroupStats()
    return _lambda(event, context)
