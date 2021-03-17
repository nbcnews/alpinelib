import alpinelib.aws.secretsmanager
from boto3 import client
from botocore.exceptions import ClientError
from kafka import KafkaProducer
from time import sleep
from alpinelib import logging

logger = logging.getFormattedLogger()
msk_client = client('kafka')
MAX_CONN_ATTEMPTS = 10

def new_producer(producer_name: str, cluster_name: str, **kwargs) -> KafkaProducer:
    """

    @param producer_name:
    @param cluster_name:
    @param kwargs:
    @return:
    """
    bootstrap_servers = _get_broker_tls_string(cluster_name)
    logger.info('Found bootstrap servers: {}'.format(bootstrap_servers))
    try:
        if bootstrap_servers:
            logger.info('Attempting to connect to bootstrap server')

            producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id=producer_name,
                                     security_protocol='SSL', api_version = (0,10,0), **kwargs)
            attempts = 0
            while not producer.bootstrap_connected() and attempts < MAX_CONN_ATTEMPTS:
                producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id=producer_name,
                                         security_protocol='SSL', api_version=(0, 10, 0), **kwargs)
                attempts+=1
                sleep(3)

            if producer.bootstrap_connected():
                return producer
            else:
                logger.exception("Unable to connect to boostrap servers after {} attempts".format(MAX_CONN_ATTEMPTS))
                raise Exception("Unable to connect to boostrap servers after {} attempts".format(MAX_CONN_ATTEMPTS))
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
        logger.info("Attempting to list clusters in region {}".format(str(session.Session().region_name)))
        clusters = msk_client.list_clusters(ClusterNameFilter=cluster_name) #['ClusterInfoList']
        logger.info("found cluster ARN: {}".format(clusters))
        if clusters:
            return clusters['ClusterInfoList'][0].get('ClusterArn')
        else:
            logger.info("Didn't find any clusters. Returning empty.")
            return ''
    except Exception as e:
        logger.exception("Failed to find a cluster with name {}".format(cluster_name))

def _get_broker_tls_string(cluster_name: str):
    cluster_arn = _get_cluster_arn(cluster_name)
    try:
        if cluster_arn:
            return msk_client.get_bootstrap_brokers(ClusterArn=cluster_arn).get('BootstrapBrokerStringTls')
        else:
            logger.info("Was not passed any cluster ARNs. Cannot find bootstrap server for nonexistent cluster.")
            return ''
    except Exception as e:
        logger.exception("Failed to find brokers for cluster {}".format(cluster_name))
        raise e


