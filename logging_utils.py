import io
import logging
import shutil
from pathlib import Path
from datetime import datetime

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


def get_today_date():
    return datetime.today().strftime(format="%d_%m_%Y_%H_%M_%S")


def move_logfile(to_dir, algo_state):
    src_file = Path.cwd() / 'ya_loader.log'
    dst_file = Path(to_dir) / f'log_{get_today_date()}_{algo_state}.log'
    shutil.copy(src_file, dst_file)
