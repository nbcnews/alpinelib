from . import logging
import gzip
import zlib

logger = logging.getFormattedLogger()

def compress_gzip(data):
    try:
        compressor = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
        compressed_data = compressor.compress(data.encode('utf-8')) + compressor.flush()
    except Exception as e:
        logger.exception("Failed to compress data.")
        raise e
    else:
        return compressed_data


def uncompress_gzip(obj):
    try:
        data = gzip.decompress(obj)
    except Exception as e:
        logger.exception("Failed to uncompress gzip")
        raise e
    else:
        return data