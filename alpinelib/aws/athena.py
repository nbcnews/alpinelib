import csv
import time

import boto3

from .. import logging

logger = logging.getFormattedLogger()
athena = boto3.client('athena')
s3 = boto3.resource('s3')


def poll_status(exetionId):
    while True:
        result = athena.get_query_execution(QueryExecutionId=exetionId)

        if result['QueryExecution']['Status']['State'] not in ['RUNNING', 'QUEUED']:
            return result

        time.sleep(5)


def query(database, query, output_bucket_name):
    """returns a table of results, first row is labels, leading and trailing quotes are removed"""
    startTime = int(round(time.time() * 1000))

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': 's3://' + output_bucket_name}
    )
    QueryExecutionId = response['QueryExecutionId']
    result = poll_status(QueryExecutionId)

    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        s3_key = QueryExecutionId + '.csv'
        obj = s3.Object(output_bucket_name, s3_key)

        query_time = str(int(round(time.time() * 1000)) - startTime)
        logger.info("Athena query {} took : {} ms".format(QueryExecutionId, query_time))

        query_result = obj.get()['Body'].read().decode('utf-8').splitlines()
        result_table = list(csv.reader(query_result, delimiter=',', quotechar='"'))

        return result_table
    else:
        logger.exception("Query {} failed. {}".format(QueryExecutionId, result))
        raise ValueError("Query {} failed. {}".format(QueryExecutionId, result))
