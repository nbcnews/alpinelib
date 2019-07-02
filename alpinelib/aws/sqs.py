from alpinelib.logger import logger
from alpinelib.aws import aws_lambda
import boto3
import uuid

sqs = boto3.resource('sqs')

def fifo_send_message(queue_name, data, group_id):
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    try:
        queue.send_message(
            MessageBody=data,
            MessageGroupId=group_id,
            MessageDeduplicationId=str(uuid.uuid4())
        )
    except Exception as e:
        logger().exception("Failed to send data to sqs {}.".format(data))
        raise e


def process_messages(queue_name, function_name):
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    try:
        while True:
            messages = queue.receive_messages(AttributeNames=['SentTimestamp'], MaxNumberOfMessages=10)
            if messages:
                for message in messages:
                    aws_lambda.invoke(function_name, message.body)
                    message.delete()
            else:
                return
    except Exception as e:
        logger().exception("Failed processing queue {}.".format(queue_name))
        raise e


def get_queue_count(queue_name):
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
    except Exception as e:
        logger().exception("Failed to get queue count")
        raise e
    else:
        return queue.attributes.get('ApproximateNumberOfMessages')
