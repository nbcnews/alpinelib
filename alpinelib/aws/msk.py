import alpinelib.aws.secretsmanager
from boto3 import client
from botocore.exceptions import ClientError
from kafka import KafkaProducer
from time import sleep
from alpinelib import logging
import json
import os

logger = logging.getFormattedLogger()
msk_client = client('kafka')
MAX_CONN_ATTEMPTS = 10


class KafkaClient:

    def __init__(self, producer_name, **kwargs):
        logger.info("Getting kafka cluster name")
        self.cluster_name = os.environ.get('kafkaClusterName')
        logger.info("getting new producer")
        self.name = producer_name
        self.params = kwargs
        self._create_producer()

    def _create_producer(self):
        self.producer = _new_producer(self.name, self.cluster_name, **self.params)

    def send(self, data: dict, topic: str):
        '''
        Takes in a dict, serializes it to JSON, and then sends it to the specified topic.
        @param data: A dict representing the data to send
        @param topic: The topic to send data to
        @return:
        '''
        # Is this needed? Probably not but figured a check might be nice?
        if not self.producer.bootstrap_connected():
            self._create_producer()
        value = json.dumps(data).encode('utf-8')
        self.producer.send(topic=topic, value=value)

    def send_raw(self,data, topic: str):
        '''
        Sends the raw data without doing any serialization or encoding.
        @param data: The data to send
        @param topic: The topic to send to
        @return:
        '''
        self.producer.send(topic=topic,value=data)

    def flush_and_close(self):
        self.producer.flush()
        logger.info("Producer Metrics: {}".format(self.producer.metrics()))
        self.producer.close()

def _new_producer(producer_name: str, cluster_name: str, **kwargs) -> KafkaProducer:
    """

    @param producer_name: The name the producer will identify itself with to the cluster
    @param cluster_name: The name of the MSK cluster in AWS
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
                sleep(3) # wait 3 seconds to try and create a new one. Could be modified to be exponential backoff

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


