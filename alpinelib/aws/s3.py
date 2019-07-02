from .. import logging
from .. import compressor
import boto3

logger = logging.getFormattedLogger()
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


def path_exists(bucket, object):
    try:
        obj = list(s3.Bucket(bucket).objects.filter(Prefix=object))
        if len(obj) > 0:
            return True
        else: return False
    except Exception as e:
        logger.exception('Failed to check if path exists')
        raise e


def read_object(bucket, key):
    try:
        obj = s3.Object(bucket, key)
    except Exception as e:
        logger.exception('Failed to read s3 object')
        raise e
    else:
        return obj.get()['Body'].read().decode('utf-8')


def get_folder_size(bucket, prefix):
    objects = s3.Bucket(bucket).objects.filter(Prefix=prefix)
    size = sum(1 for _ in objects)
    return size


def put_object(bucket_name, key, data, compression_type):
    try:
        if compression_type == 'gzip':
            data = compressor.compress_gzip(data)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data
        )
    except Exception as e:
        logger.exception("Failed s3 load to {}.".format(key))
        raise e


def move_objects(bucket, source, destination, destination_bucket='', delete_source_after_copy=True, suffix=''):
    """Move or copy objects within s3.
    Param -
        source = Source s3 location. (must have trailing slash if folder)
        destination = Destination s3 location (must have trailing slash if folder)
        destination_bucket = destination bucket name. Will use source bucket if kept blank (optional)
        delete_source_after_copy (default move)
        Suffix = Copy or move only matching files with suffix (optional)
    """

    if(destination_bucket.strip() == ''):
        destination_bucket = bucket

    try:
        for obj in filter(lambda x: x.key.endswith(suffix), s3.Bucket(name=bucket).objects.filter(Prefix=source)):
            dest_key = destination + str(obj.key)[str(obj.key).rfind('/')+1:]
            if(not destination.endswith('/')):
                dest_key = destination

            s3.Object(destination_bucket, dest_key).copy_from(CopySource={'Bucket': bucket, 'Key': obj.key})

            if (delete_source_after_copy == True):
                s3.Object(bucket, obj.key).delete()

    except Exception as e:
        logger.exception("Failed to move object(s) from {} to {} from bucket {}.".format(source, destination, bucket))
        raise e


def delete_folder(bucket, folder):
    try:
        for object in s3.Bucket(name=bucket).objects.all():
            if folder == object.key.split('/')[0]:
                s3.Object(bucket, object.key).delete()
    except Exception as e:
        logger.exception("Failed to delete folder {} from bucket {}.".format(folder, bucket,))
        raise e
