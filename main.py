import datetime
import logging
import os
from configparser import ConfigParser
from contextlib import closing
from sys import argv
from time import sleep

import matplotlib.animation as animation
import matplotlib.pyplot as plt
import pymysql
from matplotlib.ticker import AutoMinorLocator
from pymysql.cursors import DictCursor

from monitor import Monitor


def monitoring(monitor):
    with closing(pymysql.connect(host=monitor.host, port=monitor.port, user=monitor.user, password=monitor.password,
                                 db=monitor.db_name, charset='utf8', cursorclass=DictCursor)) as connection:
        start_time = monitor.start_time.strftime('%Y-%m-%d %H:%M:%S')
        last_time = monitor.last_time.strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Выполняем запросы в DB {monitor.start_time.strftime('%Y-%m-%d %H:%M:%S')} "
                     f"- {monitor.last_time.strftime('%Y-%m-%d %H:%M:%S')}")
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

    logging.info('Рассчет метрик')
    monitor.check_person_stacks(persons)
    monitor.find_metrics(persons, statuses)
    monitor.update_time()


def draw_graphs(monitor):
    fig, ax = plt.subplots(figsize=(19, 10))

    def get_data(*args):
        # 1 FT
        if not monitor.real_time and monitor.start and monitor.last_time < monitor.NOW:
            monitoring(monitor)
        # 2 TT
        else:
            monitor.real_time = True
        if monitor.real_time and monitor.start:
            monitoring(monitor)
            monitor.start = False
        # 3 TF
        elif monitor.real_time and not monitor.start:
            while datetime.datetime.now() < monitor.last_time:
                logging.debug(f'Sleep until {datetime.datetime.now()} == {monitor.last_time}')
                sleep(10)
            monitoring(monitor)

        ax.clear()
        ax.set_title(f"{monitor.country}. "
                     f"C {(monitor.NOW - datetime.timedelta(hours=monitor.time_shift)).strftime('%H:%M %d.%m.')}"
                     f" по {monitor.start_time.strftime('%H:%M %d.%m.')} "
                     f"Заявок новых клиентов - {monitor.total_bids_day}, повторных - {monitor.repeat_bids_day}"
                     f", одобрено - {monitor.approves_day}", fontsize=15)

        ax.grid(which="major", linewidth=1.2)
        ax.grid(which="minor", linestyle="--", color="gray", linewidth=0.5)

        if monitor.complete_registration_day:
            label_complete_registration_day = f"% прохождения {round(monitor.complete_registration_day[-1][1])}"
        else:
            label_complete_registration_day = "% прохождения"
        if monitor.new_bids:
            label_new_bids = f"Новые заявки {monitor.new_bids[-1][1]}"
        else:
            label_new_bids = "Новые заявки"
        if monitor.approves:
            label_approves = f"Одобрения {monitor.approves[-1][1]}"
        else:
            label_approves = "Одобрения"
        if monitor.scoring_time:
            label_scoring_time = f"Ср. время скоринга {monitor.scoring_time[-1][1]} мин."
        else:
            label_scoring_time = "Ср. время скоринга"
        if monitor.scoring_time:
            label_scoring_stuck_day = f"Застряли в скоринге {monitor.scoring_stuck_day[-1][1]}"
        else:
            label_scoring_stuck_day = "Застряли в скоринге"

        # draw graphs
        logging.info('Рисуем графики')
        plt.plot([i[0] for i in monitor.complete_registration_day],
                 [i[1] for i in monitor.complete_registration_day], 'o-', color='brown',
                 label=label_complete_registration_day)
        plt.plot([i[0] for i in monitor.scoring_stuck_day],
                 [i[1] for i in monitor.scoring_stuck_day], 'o-', color='red',
                 label=label_scoring_stuck_day)
        plt.plot([i[0] for i in monitor.new_bids],
                 [i[1] for i in monitor.new_bids], 'o-', color='blue',
                 label=label_new_bids)
        plt.plot([i[0] for i in monitor.approves],
                 [i[1] for i in monitor.approves], 'o-', color='green',
                 label=label_approves)
        plt.plot([i[0] for i in monitor.scoring_time],
                 [i[1] for i in monitor.scoring_time], 'o-', color='magenta',
                 label=label_scoring_time)

        ax.legend(loc='upper left')
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
    country = time_shift = None
    try:
        COMMAND_LINE_INPUT = eval(config['options']['COMMAND_LINE_INPUT'])
        if COMMAND_LINE_INPUT:
            if len(argv) == 3:
                country = argv[2]
            time_shift = argv[1]
        else:
            time_shift = 1
    except IndexError:
        logging.exception('Введите количество часов для построения графика!')
        raise Exception('Введите количество часов для построения графика!')
    monitor = Monitor(time_shift, country=country)
    draw_graphs(monitor)


if __name__ == '__main__':
    main()

