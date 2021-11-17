# Python Lambdas

Contents of this file

1. Setup the environment
2. Overview of lambda functions
3. Aggregates
4. Utility scripts
5. Overview of code and project structure
6. Testing framework
7. Deployment
8. Outstanding issues

## set up
```bash
source activate dyn
```

If you are using the testing framework, make sure you start the local server
first (see section on testing below).

Note this project uses Python 3.6.

## Overview of lambda functions

All lambda functions, intended to be run in the AWS environment, live in the
`src/functions` folder. Each function is a subfolder. In each folder we have:
- `main.py`: defines the code to be executed. Must contain a 
  `handle(event, context)` function at module level.
- `__init__.py`: needed since we are using Python go to 3

Next I list each function and briefly describe it. Some functions are
grouped together here. Some are redundant and are noted here. For details see
the inline code in each `main.py` file.

### calc_X_stats

For example `calc_class_stats`, `calc_teacher_stats`, ... up to `group` level.

These are fairly simple functions that iterate over each, e.g., class and 
calculate how many, e.g., students they have, saving it in the `stats` 
attribute on the record.

Since students and classes and teachers shift around from time to time, and are
also created and deleted, these need to be recalculated. This happens daily at
around midnight. Currently these functions sit on the `app.funenglishhr.cn` 
server as cron jobs. 

### hourly_X_aggregates

These functions are redundant now and should probably be deleted from the stack.

All aggregate functionality has been moved to the scripts 
`aggregate_students.py` and `aggregate_parents.py` - see the section below on
aggregates.

### module_results

This is a fairly important lambda in the LMS. It is used to present results
for a module - i.e. accuracy over words and students.






## Outstanding Issues

### Repository pattern

For some time I've wanted to implement a repository pattern for the sake of 
programming convenience. Although the `src.util.ddb_util.LambdaDDB` class is
super useful, it is still slower than it should be. For example to query the
students in a class (a very common operation) we need all this code:

```python
students = ddb.query(
    table_name='students',
    projection='class_code',
    condition='class_code = :cc',
    attr_values={':cc': {'S': _class['code']}})
``` 

Particularly cumbersome is writing in the attribute values. I started drafting
a repository pattern in `src.util.repos.py`. The ideal is to have an interface
that does all this dirty work, hiding the DynamoDB implementation details, so
the same query should be like

```python
students = ddb.students.get(
    class_code=_class['code'], 
    project='class_code')
```

This is much more reusable, cleaner, and easier to read. It also decouples the
code from our choice of database - a question that has been raised recently,
as the repository class can just be reimplemented.





