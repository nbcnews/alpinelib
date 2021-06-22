import boto3

from .. import logging

logger = logging.getFormattedLogger()
lambda_client = boto3.client('lambda', region_name='us-west-2')


def invoke(function_name, message):
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=message
        )
        return response
    except Exception as e:
        logger.exception("Failed to invoke lambda {}.".format(function_name))
        raise e
