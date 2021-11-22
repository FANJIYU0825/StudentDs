"""Script for 5minutely student aggregate calculations."""
import os
import logging
import argparse
import datetime
import boto3
from src.util import agg_util, LOGGER, date_util, mail_util


if __name__ == '__main__':
    log_dir = os.path.join(os.getcwd(), 'tmp')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    log_file_path = os.path.join(log_dir, 'aggregate_students.log')
    if os.path.exists(log_file_path):
        os.remove(log_file_path)
    file_handler = logging.FileHandler(log_file_path)
    LOGGER.handlers = []
    LOGGER.addHandler(file_handler)
    LOGGER.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument('profile_name', type=str)
    parser.add_argument('--date', type=str)
    parser.add_argument('--n_threads', type=int, default=1)
    args = parser.parse_args()
    date = args.date if args.date \
        else date_util.date_to_str(datetime.date.today())

    boto3.setup_default_session(profile_name=args.profile_name)
    _lambda = agg_util.StudentAggregates()
    event = {'path': 'cron_aggregate_students',
             'profile_name': args.profile_name,
             'n_threads': args.n_threads,
             'pathParameters': {'date': date, 'consume': True}}
    result = _lambda(event, None)
    LOGGER.info(result)

    # only send the email every 6 hours - 12am 6am 12pm 6pm
    now = datetime.datetime.now()
    if now.hour in [12, 6] and 0 <= now.minute < 5:
        if result['statusCode'] == 200:
            success = 'SUCCESS'
        else:
            success = 'FAILURE'
        body = open(log_file_path).read()
        mail_util.send('[%s] Student Aggregates' % success, body)
