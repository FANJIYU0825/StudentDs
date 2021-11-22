"""For aggregating teacher data."""
from util import lambda_util


class CalcTeacherStats(lambda_util.Lambda2):

    def function_logic(self, event, context):
        teachers = self.ddb.scan(
            table_name='teachers',
            projection='#gs,username',
            attr_names={'#gs': 'group::school'}
        )

        table = self.ddb.table('teachers')

        for teacher in teachers:
            stats = {
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

            classes = self.ddb.query(
                table_name='classes',
                projection='code,#level',
                condition='username = :un',
                attr_names={'#level': 'level'},
                attr_values={':un': {'S': teacher['username']}}
            )

            for _class in classes:
                level = _class['level']

                students = self.ddb.query(
                    table_name='students',
                    projection='student_pin',
                    condition='class_code = :cc',
                    attr_values={':cc': {'S': _class['code']}}
                )

                stats['num_classes'] += 1
                stats['num_students'] += len(students)
                stats['levels'][level]['num_classes'] += 1
                stats['levels'][level]['num_students'] += len(students)

            table.update_item(
                Key={'group::school': teacher['group::school'],
                     'username': teacher['username']},
                UpdateExpression='set stats = :s',
                ExpressionAttributeValues={':s': stats}
            )

        return lambda_util.ok({'calc_result': 'dancing'})


def handle(event, context):
    _lambda = CalcTeacherStats()
    return _lambda(event=event, context=context)
