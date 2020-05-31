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


def draw_graphs(monitor_1, monitor_2=None):
    if not monitor_2:
        fig, ax = plt.subplots(figsize=(19, 10))
    else:
        fig, axes = plt.subplots(2, 1, figsize=(19, 10))

    def get_data(*args):
        if monitor_1.start_time + datetime.timedelta(minutes=TIME_DELTA) < datetime.datetime.now():
            monitoring(monitor_1)
            if monitor_2:
                monitoring(monitor_2)
        else:
            while datetime.datetime.now() < monitor_1.last_time:
                logging.debug(f'Sleep until {datetime.datetime.now()} == {monitor_1.last_time}')
                sleep(10)
            monitoring(monitor_1)
            if monitor_2:
                monitoring(monitor_2)


        if monitor_2:
            axes[0].clear()
            axes[1].clear()
        else:
            ax.clear()
        # keep monitoring time interval up to STACK_DURATION
        start_time = (monitor_1.NOW - datetime.timedelta(hours=monitor_1.time_shift))
        delta = abs(start_time - monitor_1.last_time)
        logging.debug(f"start_time - {start_time.strftime('%H:%M:%S %d.%m.')}, "
                      f"monitor.last_time - {monitor_1.last_time.strftime('%H:%M:%S %d.%m.')}, "
                      f"delta = {delta}")
        if delta > datetime.timedelta(hours=STACK_DURATION):
            start_time = monitor_1.last_time - datetime.timedelta(hours=STACK_DURATION)
            # Если время первого запроса к БД отличается от времени текущего запроса больше, чем на STACK_DURATION
            # очищаем стеки от записей старше STACK_DURATION
            monitor_1.update_counters(start_time)
            if monitor_2:
                monitor_2.update_counters(start_time)
            logging.info(f"change monitoring time interval to {start_time.strftime('%H:%M %d.%m.')}")
        logging.debug(f"!complete_registration_day - {monitor_1.complete_registration_day}")

        if monitor_1.complete_registration_day:
            complete_registration_day_ru = round(monitor_1.complete_registration_day[-1][1], 1)
        else:
            complete_registration_day_ru = 0

        if monitor_2:
            if monitor_2.complete_registration_day:
                complete_registration_day_kz = round(monitor_2.complete_registration_day[-1][1], 1)
            else:
                complete_registration_day_kz = 0
            axes[1].set_title(f"{monitor_2.country}. C {start_time.strftime('%H:%M %d.%m.')}"
                        f" по {monitor_2.last_time.strftime('%H:%M %d.%m.')} "
                        f"Всего заявок - {monitor_2.all_bids_day}, завершенных - {monitor_2.total_bids_day}, "
                        f"повторных - {monitor_2.repeat_bids_day}, одобрено - {monitor_2.approves_day}. "
                        f"Прохождение цепочки {complete_registration_day_kz}%", fontsize=16)

            axes[0].set_title(f"{monitor_1.country}. C {start_time.strftime('%H:%M %d.%m.')}"
                         f" по {monitor_1.last_time.strftime('%H:%M %d.%m.')} "
                         f"Всего заявок - {monitor_1.all_bids_day}, завершенных - {monitor_1.total_bids_day}, "
                         f"повторных - {monitor_1.repeat_bids_day}, одобрено - {monitor_1.approves_day}. "
                         f"Прохождение цепочки {complete_registration_day_ru}%", fontsize=16)
        else:
            ax.set_title(f"{monitor_1.country}. C {start_time.strftime('%H:%M %d.%m.')}"
                        f" по {monitor_1.last_time.strftime('%H:%M %d.%m.')} "
                        f"Всего заявок - {monitor_1.all_bids_day}, завершенных - {monitor_1.total_bids_day}, "
                        f"повторных - {monitor_1.repeat_bids_day}, одобрено - {monitor_1.approves_day}. "
                        f"Прохождение цепочки {complete_registration_day_ru}%", fontsize=16)


        if monitor_2:
            axes[0].grid(which="major", linewidth=1.2)
            axes[0].grid(which="minor", linestyle="--", color="gray", linewidth=0.5)
            axes[1].grid(which="major", linewidth=1.2)
            axes[1].grid(which="minor", linestyle="--", color="gray", linewidth=0.5)
        else:
            ax.grid(which="major", linewidth=1.2)
            ax.grid(which="minor", linestyle="--", color="gray", linewidth=0.5)

        if monitor_1.new_bids:
            label_new_bids_ru = f"Новые заявки {monitor_1.new_bids[-1][1]}"
        else:
            label_new_bids_ru = "Новые заявки"
        if monitor_1.approves:
            label_approves_ru = f"Одобрения {monitor_1.approves[-1][1]}"
        else:
            label_approves_ru = "Одобрения"
        if monitor_1.scoring_time:
            label_scoring_time_ru = f"Ср. время скоринга {monitor_1.scoring_time[-1][1]} мин."
        else:
            label_scoring_time_ru = "Ср. время скоринга"
        if monitor_1.scoring_stuck_day:
            label_scoring_stuck_day_ru = f"Застряли в скоринге {monitor_1.scoring_stuck_day[-1][1]}"
        else:
            label_scoring_stuck_day_ru = "Застряли в скоринге"

        logging.info('Рисуем графики')
        if monitor_2:
            if monitor_2.new_bids:
                label_new_bids_kz = f"Новые заявки {monitor_2.new_bids[-1][1]}"
            else:
                label_new_bids_kz = "Новые заявки"
            if monitor_2.approves:
                label_approves_kz = f"Одобрения {monitor_2.approves[-1][1]}"
            else:
                label_approves_kz = "Одобрения"
            if monitor_2.scoring_time:
                label_scoring_time_kz = f"Ср. время скоринга {monitor_2.scoring_time[-1][1]} мин."
            else:
                label_scoring_time_kz = "Ср. время скоринга"
            if monitor_2.scoring_stuck_day:
                label_scoring_stuck_day_kz = f"Застряли в скоринге {monitor_2.scoring_stuck_day[-1][1]}"
            else:
                label_scoring_stuck_day_kz = "Застряли в скоринге"
            axes[1].plot([i[0] for i in monitor_2.scoring_time],
                         [i[1] for i in monitor_2.scoring_time], 'o-', color='darkviolet',
                         label=label_scoring_time_kz)
            axes[1].plot([i[0] for i in monitor_2.scoring_stuck_day],
                         [i[1] for i in monitor_2.scoring_stuck_day], 'o-', color='red',
                         label=label_scoring_stuck_day_kz)
            axes[1].plot([i[0] for i in monitor_2.new_bids],
                         [i[1] for i in monitor_2.new_bids], 'o-', color='royalblue',
                         label=label_new_bids_kz)
            axes[1].plot([i[0] for i in monitor_2.approves],
                         [i[1] for i in monitor_2.approves], 'o-', color='limegreen',
                         label=label_approves_kz)

            axes[0].plot([i[0] for i in monitor_1.scoring_time],
                         [i[1] for i in monitor_1.scoring_time], 'o-', color='darkviolet',
                         label=label_scoring_time_ru)
            axes[0].plot([i[0] for i in monitor_1.scoring_stuck_day],
                         [i[1] for i in monitor_1.scoring_stuck_day], 'o-', color='red',
                         label=label_scoring_stuck_day_ru)
            axes[0].plot([i[0] for i in monitor_1.new_bids],
                         [i[1] for i in monitor_1.new_bids], 'o-', color='royalblue',
                         label=label_new_bids_ru)
            axes[0].plot([i[0] for i in monitor_1.approves],
                         [i[1] for i in monitor_1.approves], 'o-', color='limegreen',
                         label=label_approves_ru)
        else:
            ax.plot([i[0] for i in monitor_1.scoring_time],
                         [i[1] for i in monitor_1.scoring_time], 'o-', color='darkviolet',
                         label=label_scoring_time_ru)
            ax.plot([i[0] for i in monitor_1.scoring_stuck_day],
                         [i[1] for i in monitor_1.scoring_stuck_day], 'o-', color='red',
                         label=label_scoring_stuck_day_ru)
            ax.plot([i[0] for i in monitor_1.new_bids],
                         [i[1] for i in monitor_1.new_bids], 'o-', color='royalblue',
                         label=label_new_bids_ru)
            ax.plot([i[0] for i in monitor_1.approves],
                         [i[1] for i in monitor_1.approves], 'o-', color='limegreen',
                         label=label_approves_ru)



        if monitor_2:
            axes[0].legend(loc='upper left')
            axes[0].yaxis.set_minor_locator(AutoMinorLocator())
            axes[0].tick_params(which='major', length=10, width=2)
            axes[0].tick_params(which='minor', length=5, width=1)

            axes[1].legend(loc='upper left')
            axes[1].yaxis.set_minor_locator(AutoMinorLocator())
            axes[1].tick_params(which='major', length=10, width=2)
            axes[1].tick_params(which='minor', length=5, width=1)
        else:
            ax.legend(loc='upper left')
            ax.yaxis.set_minor_locator(AutoMinorLocator())
            ax.tick_params(which='major', length=10, width=2)
            ax.tick_params(which='minor', length=5, width=1)

        monitor_1.update_time()
        if monitor_2:
            monitor_2.update_time()
        logging.debug('---------End')

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
                # specific country case
                country = argv[2]
                time_shift = argv[1]
                monitor_1 = Monitor(time_shift, country=country)
                draw_graphs(monitor_1)
            elif len(argv) == 2:
                # both countries case
                time_shift = argv[1]
                monitor_1 = Monitor(time_shift, country='ru')
                monitor_2 = Monitor(time_shift, country='kz')
                draw_graphs(monitor_1, monitor_2)
    except IndexError:
        logging.exception('Введите корректную команду!')
        raise Exception('Введите корректную команду!')


if __name__ == '__main__':
    main()

