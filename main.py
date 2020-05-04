import logging
import os
import time
from configparser import ConfigParser
from contextlib import closing
from sys import argv
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import matplotlib.animation as animation
import datetime
from time import sleep

import pymysql
import schedule
from pymysql.cursors import DictCursor

from constants import TIME_DELTA
from monitor import Monitor


def monitoring(monitor, db_name='ru_backend'):
    config = ConfigParser()
    config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
    config.read(config_file)
    with closing(pymysql.connect(host=config['db']['host'], port=int(config['db']['port']), user=config['db']['user'],
                                 password=config['db']['password'], db=db_name,
                                 charset='utf8', cursorclass=DictCursor)) as connection:
        start_time = monitor.start_time.strftime('%Y-%m-%d %H:%M:%S')
        last_time = monitor.last_time.strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Выполняем запросы в DB {monitor.start_time.strftime('%Y-%m-%d %H:%M:%S')} - {monitor.last_time.strftime('%Y-%m-%d %H:%M:%S')}")
        with connection.cursor() as cursor:
            query = f"SELECT id, status, create_ts FROM credit " \
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
    logging.info('Рассчет метрик')
    monitor.find_metrics(persons, statuses)
    # check and save credit and person stacks
    monitor.check_person_stacks(persons)
    monitor.check_credits_stack(credits)
    monitor.update_time()

def draw_graphs(monitor):
    fig, ax = plt.subplots(figsize=(8, 6))

    def get_data(*args):
        if monitor.last_time < monitor.NOW:
            monitoring(monitor)
        else:
            monitor.time_shift = True
        if monitor.time_shift and monitor.start:
            monitoring(monitor)
            monitor.start = False
        elif not monitor.time_shift and not monitor.start:
            while datetime.datetime.now() < monitor.last_time + TIME_DELTA:
                sleep(1)
            monitoring(monitor)
        monitor.time_shift = False

        ax.clear()
        ax.set_title("Россия", fontsize=16)
        ax.set_xlabel("Время", fontsize=14)
        ax.grid(which="major", linewidth=1.2)
        ax.grid(which="minor", linestyle="--", color="gray", linewidth=0.5)
        # draw graphs
        logging.info('Рисуем графики')
        plt.plot([i[0] for i in monitor.complete_registration_day],
                 [i[1] for i in monitor.complete_registration_day], 'o-', label="% прохождения")
        plt.plot([i[0] for i in monitor.scoring_stuck_day],
                 [i[1] for i in monitor.scoring_stuck_day], 'o-', label="Зависшие в скоринге")
        plt.plot([i[0] for i in monitor.new_bids],
                 [i[1] for i in monitor.new_bids], 'o-', label="Новые заявки")
        plt.plot([i[0] for i in monitor.approves],
                 [i[1] for i in monitor.approves], 'o-', label="Одобрения")
        plt.plot([i[0] for i in monitor.pastdue],
                 [i[1] for i in monitor.pastdue], 'o-', label="В просрочку")
        plt.plot([i[0] for i in monitor.pastdue_repayment],
                 [i[1] for i in monitor.pastdue_repayment], 'o-', label="Из просрочки")
        plt.plot([i[0] for i in monitor.scoring_time],
                 [i[1] for i in monitor.scoring_time], 'o-', label="Время скоринга")

        ax.legend(bbox_to_anchor=(1, 0.6))
        ax.xaxis.set_minor_locator(AutoMinorLocator())
        ax.yaxis.set_minor_locator(AutoMinorLocator())
        ax.tick_params(which='major', length=10, width=2)
        ax.tick_params(which='minor', length=5, width=1)

    ani = animation.FuncAnimation(fig, get_data)
    plt.show()

def main():
    os.makedirs('logs', exist_ok=True)
    level = logging.INFO
    handlers = [logging.FileHandler('logs/log.txt'), logging.StreamHandler()]
    format = u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s'
    logging.basicConfig(level=level, format=format, handlers=handlers)
    config = ConfigParser()
    config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
    config.read(config_file)
    time_shift = None
    try:
        COMMAND_LINE_INPUT = eval(config['options']['COMMAND_LINE_INPUT'])
        if COMMAND_LINE_INPUT:
            if len(argv) > 1:
                time_shift = argv[1]
        else:
            time_shift = 0
    except IndexError:
        logging.exception('Введите количество часов для построения графика!')
        raise Exception('Введите количество часов для построения графика!')

    monitor = Monitor(time_shift)
    draw_graphs(monitor)
    """
    if time_shift:
        count = 1
        while monitor.last_time < monitor.NOW:
            count += 1
            logging.debug(f'Запуск {count}')
            monitoring(monitor)

    schedule.every(TIME_DELTA).minutes.do(monitoring, monitor=monitor)
    #schedule.every(TIME_DELTA).minutes.do(monitoring, monitor=monitor, db_name='kz_backend')

    while True:
        schedule.run_pending()
        time.sleep(1)"""

if __name__ == '__main__':
    main()

