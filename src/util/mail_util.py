"""Utilities for emailing via AWS."""
import boto3


def send(subject, body):
    boto3.setup_default_session(profile_name='prod')
    ses = boto3.client('ses')
    response = ses.send_email(

        Message={'Subject': {'Data': subject},
                 'Body': {'Text': {'Data': body}}})
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
