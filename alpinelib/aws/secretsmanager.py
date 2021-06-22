import json

import boto3
from botocore.exceptions import ClientError

from .. import logging

logger = logging.getFormattedLogger()
sm = boto3.client('secretsmanager')


def get_secret(secret_name):
    try:
        get_secret_value_response = sm.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        logger.exception("Failed getting {} secret. ErrorCode: {}".format(secret_name, e.response['Error']['Code']))
        raise e
    else:
        return json.loads(get_secret_value_response['SecretString'])


def update_secret(secret_name, secret):
    try:
        sm.update_secret(
            SecretId=secret_name,
            SecretString=secret
        )
    except ClientError as e:
        logger.exception("Failed updating {} secret. ErrorCode: {}".format(secret_name, e.response['Error']['Code']))
        raise e
