import alpinelib.aws.secretsmanager
from boto3 import client
from botocore.exceptions import ClientError
from kafka import KafkaProducer

from alpinelib import logging

logger = logging.getFormattedLogger()
msk_client = client('kafka')


def new_producer(producer_name: str, cluster_name: str, **kwargs) -> KafkaProducer:
    """

    @param producer_name:
    @param cluster_name:
    @param kwargs:
    @return:
    """
    bootstrap_servers = _get_broker_tls_string(cluster_name)
    try:
        if bootstrap_servers:
            logger.info('Attempting to connect to bootstrap server')
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id=producer_name,
                                     security_protocol='SSL', api_version = (0,10,0), **kwargs)
            return producer
        else:
            logger.warning("Did not create producer as no bootstrap servers could be found")
            pass
    except Exception as e:
        logger.exception("Failed to connect to cluster {} with broker string {}.".format(cluster_name,bootstrap_servers))
        raise e


def _get_cluster_arn(cluster_name: str):
    """

    @param cluster_name:
    @return:
    """
    try:
        clusters = msk_client.list_clusters(ClusterNameFilter=cluster_name)['ClusterInfoList']
        if clusters:
            return clusters[0].get('ClusterArn')
        else:
            return ''
    except Exception as e:
        logger.exception("Failed to find a cluster with name {}".format(cluster_name))

def _get_broker_tls_string(cluster_name: str):
    cluster_arn = _get_cluster_arn(cluster_name)
    try:
        if cluster_arn:
            return msk_client.get_bootstrap_brokers(ClusterArn=cluster_arn).get('BootstrapBrokerStringTls')
        else:
            return ''
    except Exception as e:
        logger.exception("Failed to find brokers for cluster {}".format(cluster_name))
        raise e

