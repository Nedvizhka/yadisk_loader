import shutil
import logging
import time

from datetime import datetime
from pathlib import Path

def get_today_date():
    return datetime.today().strftime(format="%d_%m_%Y_%H_%M_%S")

def move_logfile_x(to_dir, algo_state):
    src_file = Path.cwd() / 'ya_loader.log'
    dst_file = Path(to_dir) / f'log_{get_today_date()}_{algo_state}.log'
    shutil.copy(src_file, dst_file)
    open(src_file, 'w').close()

local_save_dir_data = Path.cwd()/'saved_data_test'

j = 0
while j < 4:
    logging.basicConfig(filename='ya_loader.log', filemode='w', level=logging.INFO,
                        format='%(asctime)s [%(levelname)-8s] %(message)s', encoding='utf8')

    # logging.basicConfig(filename=(Path.cwd() / 'ya_loader.log').as_posix(), filemode='w', encoding='utf8',
                        # level=logging.INFO, format='%(asctime)s [%(levelname)-8s] %(message)s')
    logging.info('None {}: abc по руссуи 123? ююб.я &')
    logging.error('кто zhe kto я')
    log = logging.getLogger()
    for i in range(len(log.handlers)):
        log.removeHandler(log.handlers[i])
    move_logfile_x(local_save_dir_data, 'success')
    time.sleep(1)
    j += 1