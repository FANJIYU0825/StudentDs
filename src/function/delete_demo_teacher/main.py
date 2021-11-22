"""Delete a demo teacher."""
from util import lambda_util


class DeleteDemoTeacher(lambda_util.Lambda2):

    def expected_path_params(self):
        return ['username']

    def function_logic(self, event, context):
        username, = self.get_path_parameters(event)

        # to be deleted
        student_assignments = []
        user_accounts = []
        students = []

        # find the teacher
        teachers = self.ddb.scan(table_name='teachers')
        teacher = next((t for t in teachers
                        if 'displayName' in t.keys()
                        and t['displayName'] == username),
                       None)
        if teacher is None:
            raise Exception('Teacher not found.')
        user_accounts.append(self.ddb.query_one(
            table_name='user-accounts',
            condition='username = :un',
            attr_values={':un': {'S': teacher['username']}}))

        classes = self.ddb.query(
            table_name='classes',
            condition='username = :tn',
            attr_values={':tn': {'S': teacher['username']}})

        for c in classes:
            students += self.ddb.query(
                table_name='students',
                condition='class_code = :cc',
                attr_values={':cc': {'S': c['code']}})
            student_assignments += self.ddb.query(
                table_name='student-assignments',
                condition='class_code = :cc',
                attr_values={':cc': {'S': c['code']}})

        experience = []
        for s in students:
            experience += self.ddb.query(
                table_name='experience',
                condition='usr = :sp',
                index_name='usr-date-index',
                attr_values={':sp': {'S': s['student_pin']}})
            user_accounts.append(self.ddb.query_one(
                table_name='user-accounts',
                condition='username = :un',
                attr_values={':un': {'S': s['student_pin']}}))

        self.ddb.delete(experience, 'experience', sleep=False)
        self.ddb.delete(student_assignments, 'student-assignments', sleep=False)
        self.ddb.delete(user_accounts, 'user-accounts', sleep=False)
        self.ddb.delete(students, 'students', sleep=False)
        self.ddb.delete(classes, 'classes', sleep=False)
        self.ddb.delete([teacher], 'teachers', sleep=False)

        return lambda_util.ok({'result': 'dancing'})


def handle(event, context):
    _lambda = DeleteDemoTeacher()
    return _lambda(event, context)
