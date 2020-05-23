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

from constants import STACK_DURATION, TIME_DELTA
from monitor import Monitor


def monitoring(monitor):
    with closing(pymysql.connect(host=monitor.host, port=monitor.port, user=monitor.user, password=monitor.password,
                                 db=monitor.db_name, charset='utf8', cursorclass=DictCursor)) as connection:
        monitor.first_monitoring = False
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

    logging.info('----------Рассчет метрик')
    monitor.check_person_stacks(persons)
    monitor.find_metrics(persons, statuses)


def draw_graphs(monitor_ru, monitor_kz):
    fig, axes = plt.subplots(2, 1, figsize=(19, 10))

    def get_data(*args):
        if monitor_ru.start_time + datetime.timedelta(minutes=TIME_DELTA) < datetime.datetime.now():
            monitoring(monitor_ru)
            monitoring(monitor_kz)
        else:
            while datetime.datetime.now() < monitor_ru.last_time:
                logging.debug(f'Sleep until {datetime.datetime.now()} == {monitor_ru.last_time}')
                sleep(10)
            monitoring(monitor_ru)
            monitoring(monitor_kz)

        axes[0].clear()
        axes[1].clear()
        # keep monitoring time interval up to STACK_DURATION
        start_time = (monitor_ru.NOW - datetime.timedelta(hours=monitor_ru.time_shift))
        delta = abs(start_time - monitor_ru.last_time)
        logging.debug(f"start_time - {start_time.strftime('%H:%M:%S %d.%m.')}, "
                      f"monitor.last_time - {monitor_ru.last_time.strftime('%H:%M:%S %d.%m.')}, "
                      f"delta = {delta}")
        if delta > datetime.timedelta(hours=STACK_DURATION):
            start_time = monitor_ru.last_time - datetime.timedelta(hours=STACK_DURATION)
            # Если время первого запроса к БД отличается от времени текущего запроса больше, чем на STACK_DURATION
            # очищаем стеки от записей старше STACK_DURATION
            monitor_ru.update_counters(start_time)
            monitor_kz.update_counters(start_time)
            logging.info(f"change monitoring time interval to {start_time.strftime('%H:%M %d.%m.')}")
        logging.debug(f"!complete_registration_day - {monitor_ru.complete_registration_day}")

        if monitor_ru.complete_registration_day:
            complete_registration_day_ru = round(monitor_ru.complete_registration_day[-1][1], 1)
        else:
            complete_registration_day_ru = 0

        if monitor_kz.complete_registration_day:
            complete_registration_day_kz = round(monitor_kz.complete_registration_day[-1][1], 1)
        else:
            complete_registration_day_kz = 0

        axes[0].set_title(f"{monitor_ru.country}. C {start_time.strftime('%H:%M %d.%m.')}"
                     f" по {monitor_ru.last_time.strftime('%H:%M %d.%m.')} "
                     f"Заявок новых клиентов - {monitor_ru.total_bids_day}, повторных - {monitor_ru.repeat_bids_day}"
                     f", одобрено - {monitor_ru.approves_day}. "
                     f"Прохождение цепочки {complete_registration_day_ru}%", fontsize=16)

        axes[1].set_title(f"{monitor_kz.country}. C {start_time.strftime('%H:%M %d.%m.')}"
                      f" по {monitor_kz.last_time.strftime('%H:%M %d.%m.')} "
                      f"Заявок новых клиентов - {monitor_kz.total_bids_day}, повторных - {monitor_kz.repeat_bids_day}"
                      f", одобрено - {monitor_kz.approves_day}. "
                      f"Прохождение цепочки {complete_registration_day_kz}%", fontsize=16)

        axes[0].grid(which="major", linewidth=1.2)
        axes[0].grid(which="minor", linestyle="--", color="gray", linewidth=0.5)

        axes[1].grid(which="major", linewidth=1.2)
        axes[1].grid(which="minor", linestyle="--", color="gray", linewidth=0.5)

        if monitor_ru.new_bids:
            label_new_bids_ru = f"Новые заявки {monitor_ru.new_bids[-1][1]}"
        else:
            label_new_bids_ru = "Новые заявки"
        if monitor_ru.approves:
            label_approves_ru = f"Одобрения {monitor_ru.approves[-1][1]}"
        else:
            label_approves_ru = "Одобрения"
        if monitor_ru.scoring_time:
            label_scoring_time_ru = f"Ср. время скоринга {monitor_ru.scoring_time[-1][1]} мин."
        else:
            label_scoring_time_ru = "Ср. время скоринга"
        if monitor_ru.scoring_stuck_day_ru:
            label_scoring_stuck_day_ru = f"Застряли в скоринге {monitor_ru.scoring_stuck_day[-1][1]}"
        else:
            label_scoring_stuck_day_ru = "Застряли в скоринге"

        if monitor_kz.new_bids:
            label_new_bids_kz = f"Новые заявки {monitor_kz.new_bids[-1][1]}"
        else:
            label_new_bids_kz = "Новые заявки"
        if monitor_kz.approves:
            label_approves_kz = f"Одобрения {monitor_kz.approves[-1][1]}"
        else:
            label_approves_kz = "Одобрения"
        if monitor_kz.scoring_time:
            label_scoring_time_kz = f"Ср. время скоринга {monitor_kz.scoring_time[-1][1]} мин."
        else:
            label_scoring_time_kz = "Ср. время скоринга"
        if monitor_kz.scoring_stuck_day_kz:
            label_scoring_stuck_day_kz = f"Застряли в скоринге {monitor_kz.scoring_stuck_day[-1][1]}"
        else:
            label_scoring_stuck_day_kz = "Застряли в скоринге"

        logging.info('Рисуем графики')
        axes[0].plot([i[0] for i in monitor_ru.scoring_time],
                 [i[1] for i in monitor_ru.scoring_time], 'o-', color='darkviolet',
                 label=label_scoring_time_ru)
        axes[0].plot([i[0] for i in monitor_ru.scoring_stuck_day],
                 [i[1] for i in monitor_ru.scoring_stuck_day], 'o-', color='red',
                 label=label_scoring_stuck_day_ru)
        axes[0].plot([i[0] for i in monitor_ru.new_bids],
                 [i[1] for i in monitor_ru.new_bids], 'o-', color='royalblue',
                 label=label_new_bids_ru)
        axes[0].plot([i[0] for i in monitor_ru.approves],
                 [i[1] for i in monitor_ru.approves], 'o-', color='limegreen',
                 label=label_approves_ru)

        axes[1].plot([i[0] for i in monitor_kz.scoring_time],
                     [i[1] for i in monitor_kz.scoring_time], 'o-', color='darkviolet',
                     label=label_scoring_time_kz)
        axes[1].plot([i[0] for i in monitor_kz.scoring_stuck_day],
                     [i[1] for i in monitor_kz.scoring_stuck_day], 'o-', color='red',
                     label=label_scoring_stuck_day_kz)
        axes[1].plot([i[0] for i in monitor_kz.new_bids],
                     [i[1] for i in monitor_kz.new_bids], 'o-', color='royalblue',
                     label=label_new_bids_kz)
        axes[1].plot([i[0] for i in monitor_kz.approves],
                     [i[1] for i in monitor_kz.approves], 'o-', color='limegreen',
                     label=label_approves_kz)

        axes[0].legend(loc='upper left')
        axes[0].yaxis.set_minor_locator(AutoMinorLocator())
        axes[0].tick_params(which='major', length=10, width=2)
        axes[0].tick_params(which='minor', length=5, width=1)

        axes[1].legend(loc='upper left')
        axes[1].yaxis.set_minor_locator(AutoMinorLocator())
        axes[1].tick_params(which='major', length=10, width=2)
        axes[1].tick_params(which='minor', length=5, width=1)

        monitor_ru.update_time()
        monitor_kz.update_time()

    ani = animation.FuncAnimation(fig, get_data)
    plt.show()


def main():
    os.makedirs('logs', exist_ok=True)
    level = logging.DEBUG
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
            elif len(argv) == 2:
                time_shift = argv[1]
            elif len(argv) == 1:
                time_shift = 0
        else:
            time_shift = 1
    except IndexError:
        logging.exception('Введите количество часов для построения графика!')
        raise Exception('Введите количество часов для построения графика!')
    monitor_ru = Monitor(time_shift, country='ru')
    monitor_kz = Monitor(time_shift, country='kz')
    draw_graphs(monitor_ru, monitor_kz)


if __name__ == '__main__':
    main()

