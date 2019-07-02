from alpinelib.logger import logger
import boto3

comprehend = boto3.client('comprehend')

def detect_entities(text):
    try:
        entities = comprehend.detect_entities(Text=text, LanguageCode='en')
    except Exception as e:
        logger().exception("Failed calling comprehend entities.")
        raise e
    else:
        if entities is None: return {}
        # Removing responseMetadata - value not meaningful and schema returned is inconsistent
        entities.pop('ResponseMetadata', None)
        return entities


def detect_sentiment(text):
    try:
        sentiment = comprehend.detect_sentiment(Text=text, LanguageCode='en')
    except Exception as e:
        logger().exception("Failed calling comprehend sentiment.")
        raise e
    else:
        if sentiment is None: return {}
        # Removing responseMetadata - value not meaningful and schema returned is inconsistent
        sentiment.pop('ResponseMetadata', None)
        return sentiment

