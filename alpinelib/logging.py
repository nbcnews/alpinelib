from pythonjsonlogger import jsonlogger
from splunk_hec_handler import SplunkHecHandler
from alpinelib.aws import secretsmanager as sm
import logging
import os


def getFormattedLogger(enable_splunk=False):
    """
    logger.error("exc_info", exc_info=True)
    logger.exception("exc_info")
    :return:
    """
    logger = logging.getLogger()

    # Add default handler
    if(not logger.handlers):
        ch = logging.StreamHandler()
        ch.set_name = 'default_stream'
        logger.addHandler(ch)

    # Add JSON formatted logger
    handler = logging.StreamHandler()
    handler.setFormatter(json_formatter())
    handler.set_name('default_json')

    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # Add or remove splunk handler
    # If enabled and not set
    try:
        if enable_splunk and 'splunk' not in [h.name for h in logger.handlers]:
            hostname = os.getenv('serviceName')
            splunk_handler = __create_splunk_handler(hostname)
            logger.addHandler(splunk_handler)

        # if disabled and already set
        elif not enable_splunk and 'splunk' in [h.name for h in logger.handlers]:
            splunk_handler = [h for h in logger.handlers if h.name == 'splunk'][0]
            logger.removeHandler(splunk_handler)
    except Exception as e:
        logger.warning('Unable to add splunk handler. Logs will not be written. Reason is: {}'.format(e))

    return logger

def __create_splunk_handler(hostname='UNKNOWN_INGEST'):
    # could probably remove the default host, as it will error
    HOST = os.getenv('SPLUNK_HOST') or sm.get_secret('splunk')['HEC_HOST']
    TOKEN = os.getenv('SPLUNK_HEC_TOKEN') or sm.get_secret('splunk')['HEC_TOKEN']
    handler = SplunkHecHandler(host=HOST, token=TOKEN, port=443, proto='HTTPS',
                               source='Alpine', hostname=hostname)
    handler.setFormatter(json_formatter())
    handler.set_name('splunk')
    return handler

def json_formatter():
    """
    Reference: https://docs.python.org/3/library/logging.html#logrecord-attributes
    """
    supported_keys = [
        'levelname',
        'asctime',
        'aws_request_id',
        'module',
        'message',
        'exc_info'
    ]

    def log_format(x):
        return ['%({0:s})'.format(i) for i in x]

    custom_format = ' '.join(log_format(supported_keys))

    return jsonlogger.JsonFormatter(custom_format)
