import logging
import os
import time
from configparser import ConfigParser
from contextlib import closing
from sys import argv

import pymysql
import schedule
from pymysql.cursors import DictCursor

from constants import TIME_DELTA
from monitor import Monitor


def db_queries(monitor, start_time=None):
    """ Получает временной отрзок и делает запросы к DB """
    config = ConfigParser()
    config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
    config.read(config_file)
    with closing(pymysql.connect(host=config['db']['host'], port=int(config['db']['port']), user=config['db']['user'],
                                 password=config['db']['password'], db=config['db']['db_name'],
                                 charset='utf8', cursorclass=DictCursor)) as connection:
        start_time = monitor.start_time.strftime('%Y-%m-%d %H:%M:%S')
        last_time = monitor.last_time.strftime('%Y-%m-%d %H:%M:%S')
        logging.INFO('Выполняем запросы в DB')
        with connection.cursor() as cursor:
            query = f"SELECT id, status, amount, returnsumm, create_ts FROM credit " \
                    f"where create_ts > '{start_time}' and create_ts < '{last_time}'"
            cursor.execute(query)
            credits = []
            for row in cursor:
                credits.append(row)
        with connection.cursor() as cursor:
            query = f"SELECT id, stage, create_ts FROM person " \
                    f"where create_ts > '{start_time}' and create_ts < '{last_time}'"
            cursor.execute(query)
            persons = []
            for row in cursor:
                persons.append(row)
        with connection.cursor() as cursor:
            query = f"SELECT credit_id, `from`, `to`, timestamp FROM h_credit_status " \
                    f"where timestamp > '{start_time}' and timestamp < '{last_time}'"
            cursor.execute(query)
            statuses = []
            for row in cursor:
                statuses.append(row)
        # find metrics
        logging.INFO('Рассчет метрик')
        monitor.find_metrics(credits, persons, statuses)
        # check and save credit and person stacks
        monitor.check_person_stacks(persons)
        monitor.check_credits_stack(credits)

        #todo draw graphs
        #todo update time



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

    monitor = Monitor(start_time)
    #todo change time interval
    schedule.every(TIME_DELTA).seconds.do(db_queries, monitor=monitor, start_time=start_time)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    #main()
    config = ConfigParser()
    config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
    config.read(config_file)
    monitor = Monitor()
    with closing(pymysql.connect(host=config['db']['host'], port=int(config['db']['port']), user=config['db']['user'],
                                 password=config['db']['password'], db=config['db']['db_name'],
                                 charset='utf8', cursorclass=DictCursor)) as connection:
        start_time = monitor.start_time.strftime('%Y-%m-%d %H:%M:%S')
        last_time = monitor.last_time.strftime('%Y-%m-%d %H:%M:%S')
        with connection.cursor() as cursor:
            query = f"SELECT id, status, amount, returnsumm, create_ts FROM credit " \
                    f"where create_ts > '{start_time}' and create_ts < '{last_time}'"
            cursor.execute(query)
            credits = []
            for row in cursor:
                credits.append(row)
        with connection.cursor() as cursor:
            query = f"SELECT id, stage, create_ts FROM person " \
                    f"where create_ts > '{start_time}' and create_ts < '{last_time}'"
            cursor.execute(query)
            persons = []
            for row in cursor:
                persons.append(row)
        with connection.cursor() as cursor:
            query = f"SELECT credit_id, `from`, `to`, timestamp FROM h_credit_status " \
                    f"where timestamp > '{start_time}' and timestamp < '{last_time}'"
            cursor.execute(query)
            statuses = []
            for row in cursor:
                statuses.append(row)
        print('credit', credits)
        print('person', persons)
        print('hcs', statuses)
        pass

