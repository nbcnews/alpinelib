import logging

from pythonjsonlogger import jsonlogger


def getFormattedLogger():
    """
    logger.error("exc_info", exc_info=True)
    logger.exception("exc_info")
    :return:
    """
    logger = logging.getLogger()

    if not logger.handlers:
        ch = logging.StreamHandler()
        logger.addHandler(ch)

    handler = logger.handlers[0]
    handler.setFormatter(json_formatter())

    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    return logger


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
