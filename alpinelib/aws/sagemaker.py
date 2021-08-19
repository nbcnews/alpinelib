import json

import boto3


def run_inference(ml_endpoint: str, bucket: str, file: str):
    """
    Sends the bucket and file name to the ml_endpoint and returns the inference response
    :param ml_endpoint: the endpoint to send data to for inference
    :param bucket: the bucket where the inference data lives
    :param file: the file where the inference data lives
    :returns: a dictionary of the inference response
    """
    sagemaker_client = boto3.client('sagemaker-runtime')

    body = {
        'data_bucket': bucket,
        'data_file': file
    }

    response = sagemaker_client.invoke_endpoint(
        EndpointName=ml_endpoint,
        Body=json.dumps(body),
        ContentType='application/json'
    )

    response_body = json.loads(response['Body'].read().decode('utf-8'))

    return response_body
