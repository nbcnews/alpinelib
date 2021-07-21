import json
import re
from typing import List

import boto3
import numpy as np

from nltk.corpus import stopwords


def __preprocess_text(text: str) -> List[str]:
    """
    This function tokenizes the text into words then lowercases and strips punctuation from it
    :param text: string of text to preprocess
    :return: the tokenized and formatted string as a list
    """
    return_list = []

    # Remove words in text that don't matter for NLP purposes
    stop_words = set(stopwords.words('english'))

    for word in text.split(' '):
        stripped_text = re.sub(r'[^\w\s]', '', word.lower())
        if len(stripped_text) > 0 and stripped_text not in stop_words:
            return_list.append(stripped_text)

    return return_list


def __postprocess_text(vectors: List[dict]) -> List[float]:
    """
    Normalize the ml response by returning the mean vector
    :param vectors: the body of the ml response. The dict should contain a key called 'vector'
    :return: the mean vector
    """
    return np.array(
        [vector['vector'] for vector in vectors]
    ).mean(axis=0).tolist()


def send_to_ml_preprocess(ml_endpoint: str, article_body: str) -> List[float]:
    """
    Sends the data in article_body to the ml_endpoint and returns the response
    :param ml_endpoint: the endpoint to send data to for inference
    :param article_body: the body as a string of the article to preprocess
    :returns: a dictionary of the inference response
    """
    sagemaker_client = boto3.client('sagemaker-runtime')

    body = {
        'instances': __preprocess_text(article_body)
    }

    response = sagemaker_client.invoke_endpoint(
        EndpointName=ml_endpoint,
        Body=json.dumps(body),
        ContentType='application/json'
    )

    response_body = json.loads(response['Body'].read().decode('utf-8'))

    return __postprocess_text(response_body)
