import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import time
import io

from test_logging_deeper import *


class TqdmToLogger(io.StringIO):
    """
        Output stream for TQDM which will output to logger module instead of
        the StdOut.
    """
    logger = None
    level = None
    buf = ''

    def __init__(self, logger, level=None):
        super(TqdmToLogger, self).__init__()
        self.logger = logger
        self.level = level or logging.INFO

    def write(self, buf):
        self.buf = buf.strip('\r\n\t ')

    def flush(self):
        self.logger.log(self.level, self.buf)


def log_test_other(val):
    try:
        time.sleep(2)
        logging.info(val)
        logging.info(val / 2)
        return 2 / val
    except Exception as exc:
        logging.error(exc)
        callfunc()
        return None


def log_test_tqdm(val):
    list_odd = []

    logger = logging.getLogger()
    tqdm_out = TqdmToLogger(logger, level=logging.INFO)

    for i in tqdm(range(0, val), file=tqdm_out, mininterval=10):
        time.sleep(3)
        if i % 2 == 0:
            # logging.info(f'четное {i}')
            list_odd.append(i)
        else:
            pass
    return list_odd
