import logging
import os
import time
from configparser import ConfigParser
from sys import argv
from contextlib import closing

import pymysql
from pymysql.cursors import DictCursor
import schedule

from monitor import Monitor


def job(monitor, start_time=None):
    config = ConfigParser()
    config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
    config.read(config_file)
    with closing(pymysql.connect(host=config['db']['host'], port=int(config['db']['port']), user=config['db']['user'],
                                 password=config['db']['password'], db=config['db']['db_name'],
                                 charset='utf8', cursorclass=DictCursor)) as connection:
        with connection.cursor() as cursor:
            query = """
                            SELECT
                                status
                            FROM
                                credit
                            Where 
                                id < 3
                            """
            cursor.execute(query)
            for row in cursor:
                print(row)



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
    #main()
    config = ConfigParser()
    config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
    config.read(config_file)
    with closing(pymysql.connect(host=config['db']['host'], port=int(config['db']['port']), user=config['db']['user'],
                                 password=config['db']['password'], db=config['db']['db_name'],
                                 charset='utf8', cursorclass=DictCursor)) as connection:
        with connection.cursor() as cursor:
            query = """
                                SELECT
                                    status
                                FROM
                                    credit
                                Where 
                                    id < 7
                                """
            cursor.execute(query)
            for row in cursor:
                print(row)
