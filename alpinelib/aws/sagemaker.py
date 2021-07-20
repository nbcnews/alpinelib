import json

import boto3


def send_to_ml_preprocess(ml_endpoint: str, article_body: str):
    """
    Sends the data in article_body to the ml_endpoint and returns the response
    :param ml_endpoint: the endpoint to send data to for inference
    :param article_body: the body as a string of the article to preprocess
    :returns: a dictionary of the inference response
    """
    sagemaker_client = boto3.client('sagemaker-runtime')

    body = {
        'instances': article_body.split(' ')
    }

    response = sagemaker_client.invoke_endpoint(
        EndpointName=ml_endpoint,
        Body=json.dumps(body),
        ContentType='application/json'
    )

    return json.loads(response['Body'].read().decode('utf-8'))
