"""Script for aggregating classes up to school-groups."""
import os
import logging
import argparse
import datetime
import boto3
from tqdm import tqdm
from src.util import agg_util, LOGGER, date_util, mail_util


def get_log_file_path(_type):
    log_dir = os.path.join(os.getcwd(), 'tmp')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    log_file_path = os.path.join(log_dir, 'aggregate_%s.log' % _type)
    if os.path.exists(log_file_path):
        os.remove(log_file_path)
    return log_file_path


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('profile_name', type=str)
    parser.add_argument('--date', type=str)
    parser.add_argument('--n_threads', type=int, default=1)
    args = parser.parse_args()
    date = args.date if args.date \
        else date_util.date_to_str(datetime.date.today())

    boto3.setup_default_session(profile_name=args.profile_name)
    event = {'path': 'cron_aggregate_parents',
             'profile_name': args.profile_name,
             'n_threads': args.n_threads,
             'pathParameters': {'date': date}}

    success = 'SUCCESS'
    exception = None

    with tqdm(total=4, desc='Table') as pbar:
        for _type in ['classes', 'teachers', 'schools', 'school-groups']:
            try:
                error = False
                pbar.set_description(_type)
                log_file_path = get_log_file_path(_type)
                file_handler = logging.FileHandler(log_file_path)
                LOGGER.handlers = []
                LOGGER.addHandler(file_handler)
                LOGGER.setLevel(logging.INFO)
                _lambda = agg_util.get_parent_lambda(_type=_type)
                result = _lambda(event, None)
                LOGGER.info('%s result:' % _type)
                LOGGER.info(result)
                if result['statusCode'] != 200:
                    raise Exception
                pbar.update()
            except:
                error = True
            finally:
                body = open(log_file_path).read()
                success = 'FAILURE' if error else 'SUCCESS'
                mail_util.send(
                    '[%s] %s' % (success, _lambda.__class__.__name__), body)
                if error:
                    break
