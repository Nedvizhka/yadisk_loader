import io
import logging as logging_module
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
        self.level = level or logging_module.INFO

    def write(self, buf):
        self.buf = buf.strip('\r\n\t ')

    def flush(self):
        self.logger.log(self.level, self.buf)


def get_today_date():
    return datetime.today().strftime(format="%d_%m_%Y_%H_%M_%S")


def move_logfile(to_dir, algo_state, logging, env_value):
    logging.info('сохранение лог-файла')
    src_file = Path.cwd() / f'ya_loader{"_"+env_value if env_value != None else ""}.log'
    dst_file = Path(to_dir) / f'saved_logs/log_{get_today_date()}{"_"+env_value if env_value != None else ""}_{algo_state}.log'
    shutil.copy(src_file, dst_file)
    print('лог сохранен')
    # Path.unlink(src_file)

    # with io.open(src_file, 'w', encoding='utf8') as file:
        # file.close()
