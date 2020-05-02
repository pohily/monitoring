import logging
import os
import time
from configparser import ConfigParser
from sys import argv

import schedule

from monitor import Monitor


def job(monitor, start_time=None):
    if start_time:
        print(start_time)
    monitor.test()


def main():
    os.makedirs('logs', exist_ok=True)
    level = logging.INFO
    handlers = [logging.FileHandler('logs/log.txt'), logging.StreamHandler()]
    format = u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s'
    logging.basicConfig(level=level, format=format, handlers=handlers)
    config = ConfigParser()
    config.read('config.ini')
    start_time = None
    try:
        COMMAND_LINE_INPUT = eval(config['options']['COMMAND_LINE_INPUT'])
        if COMMAND_LINE_INPUT:
            if len(argv) > 1:
                start_time = argv[1]
    except IndexError:
        logging.exception('Введите правильное время!')
        raise Exception('Введите правильное время!')

    monitor = Monitor()
    schedule.every(5).seconds.do(job, monitor=monitor, start_time=start_time)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    main()