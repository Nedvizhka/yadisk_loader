import shutil
import logging
import time
import io

from datetime import datetime
from pathlib import Path


class TqdmToLoggerX(io.StringIO):
    """
        Output stream for TQDM which will output to logger module instead of
        the StdOut.
    """
    logger = None
    level = None
    buf = ''

    def __init__(self, logger, level=None):
        super(TqdmToLoggerX, self).__init__()
        self.logger = logger
        self.level = level or logging.INFO

    def write(self, buf):
        self.buf = buf.strip('\r\n\t ')

    def flush(self):
        self.logger.log(self.level, self.buf)


def get_today_date():
    return datetime.today().strftime(format="%d_%m_%Y_%H_%M_%S")

# def move_logfile_x(to_dir, algo_state):
#     logging.info('сохранение лог-файла')
#     src_file = Path.cwd() / 'ya_loader.log'
#     dst_file = Path(to_dir) / f'log_{get_today_date()}_{algo_state}.log'
#     shutil.copy(src_file, dst_file)
#     logff = logging.getLogger()
#     for i in range(len(logff.handlers)):
#         logff.removeHandler(logff.handlers[i])
#     Path.unlink(src_file)
#
#     with io.open(src_file, 'w', encoding='utf8') as file:
#         file.close()

    # fp = open(src_file, 'w')
    # fp.close()

local_save_dir_data = Path.cwd()/'saved_data_test'

j = 0

while j < 4:
    logging.basicConfig(filename='ya_loader.log', filemode='w', level=logging.INFO,
                        format='%(asctime)s [%(levelname)-8s] %(message)s', encoding='utf8')

    # logging.basicConfig(filename=(Path.cwd() / 'ya_loader.log').as_posix(), filemode='w', encoding='utf8',
                        # level=logging.INFO, format='%(asctime)s [%(levelname)-8s] %(message)s')
    logging.info('None {}: abc по руссуи 123? ююб.я &')
    logging.error('кто zhe kto я')

    logger = logging.getLogger()
    tqdm_out = TqdmToLoggerX(logger, level=logging.INFO)
    tqdm_out.flush()

    move_logfile_x(local_save_dir_data, 'success')
    time.sleep(1)
    j += 1