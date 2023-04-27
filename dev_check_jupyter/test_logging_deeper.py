import logging
import time

def callfunc():
    time.sleep(3)
    logging.info('called f from 2 lvl')
    return None