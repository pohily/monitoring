import datetime
from decimal import Decimal
import os
from configparser import ConfigParser
import logging

import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator

from constants import TIME_DELTA, STACK_DURATION, KZ, RU


class Monitor():
    def __init__(self, time_shift=None, country=None):
        ############## counters
        self.approves_day = 0               # Текущее общее количество одобрений за сутки (STACK_DURATION)
        self.repeat_bids_day = 0            # Текущее общее количество повторных заявок за сутки (STACK_DURATION)
        self.total_bids_day = 0             # Текущее общее количество заявок за сутки (STACK_DURATION)
        ############## stacks
        self.stage_6_stack = {}             # persons on stage 6 за сутки (STACK_DURATION)
        self.stage_6_stack_prev = 0         # persons on stage 6 за сутки (STACK_DURATION) в предыдущий период
        self.except_6_stack = {}            # persons on stage < 6 за сутки (STACK_DURATION)
        self.scoring_stuck_stack = {}       # credits stuck in scoring
        ############## metrics
        self.complete_registration_day = [] # Текущий % прохождения цепочки за сутки (STACK_DURATION)
        self.scoring_stuck_day = []         # Текущее количество кредитов зависших на скоринге за сутки (STACK_DURATION)
        self.scoring_time = []              # среднее время скоринга за TIME_DELTA - в минутах
        self.new_bids = []                  # Количество новых заявок за TIME_DELTA
        self.approves = []                  # Количество одобрений за TIME_DELTA
        self.repeat_bids = []               # Количество повторных заявок за TIME_DELTA
        self.total_bids = []                # Количество заявок за TIME_DELTA
        ############## под вопросом
        #self.partner_bids = []              # количество заявок через партнеров за TIME_DELTA

        if not country or country in RU:
            self.country = 'Россия'
            self.db_name = 'ru_backend'
        else:
            self.country = 'Казахстан'
            self.db_name = 'kz_backend'

        config = ConfigParser()
        config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
        config.read(config_file)
        self.host = config['db']['host']
        self.port = int(config['db']['port'])
        self.user = config['db']['user']
        self.password = config['db']['password']

        self.first_monitoring = True # flag for updating time

        self.NOW = datetime.datetime.now()
        self.time_shift = None

        self.start = True       # первый раз данные получаются без задержки
        self.real_time = False  # флаг выполненного time_shift
        if time_shift:
            self.time_shift = int(time_shift)
            self.start_time = self.NOW - datetime.timedelta(hours=self.time_shift)
            self.last_time = self.start_time + datetime.timedelta(minutes=TIME_DELTA)
        else:
            self.last_time = self.NOW
            self.start_time = self.NOW - datetime.timedelta(minutes=TIME_DELTA)

    @staticmethod
    def remove_old(stack, last_time, name, value_name, time_name):
        to_del = []
        if stack:
            for key, value in stack.items():
                if abs(last_time - value[time_name]) > datetime.timedelta(hours=STACK_DURATION):
                    logging.debug(f"Remove {value[value_name]} from {name}")
                    to_del.append(key)
            for key in to_del:
                del stack[key]

    def update_time(self):
        self.start_time = self.last_time
        self.last_time = self.last_time + datetime.timedelta(minutes=TIME_DELTA)

    def update_counters(self, start_time):
        pass

    def find_metrics(self, persons, statuses):
        self.new_bids.append((self.start_time, len(persons)))
        total_bids, approves, scoring_time = 0, 0, []
        total_bids_day_prev = self.total_bids_day
        for status in statuses:
            # credit goes to scoring
            if status['from'] == 0 and status['to'] == 0:
                total_bids += 1
                self.scoring_stuck_stack[status['credit_id']] = status
                logging.debug(f"append {status} to scoring_stuck_stack")
            if status['from'] == 1 and status['to'] == 2:
                approves += 1
                self.approves_day += 1
            if status['to']:
                # scoring_time
                if status['credit_id'] in self.scoring_stuck_stack:
                    # превращаем timedelta в количество минут
                    delta = str(status['timestamp'] - self.scoring_stuck_stack[status['credit_id']]['timestamp']).split(':')
                    scoring_time.append(round(int(delta[0]) * 60 + int(delta[1]) + int(delta[2]) / 60, 1))
                    logging.debug(f"scoring_time for {status['credit_id']} "
                                  f"from {self.scoring_stuck_stack[status['credit_id']]['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}"
                                  f"to {status['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                    del self.scoring_stuck_stack[status['credit_id']]
                    logging.debug(f"Remove {status['credit_id']} from scoring_stuck_stack")
        self.total_bids_day += total_bids
        self.total_bids.append((self.start_time, total_bids))
        repeat_bids = self.total_bids_day - total_bids_day_prev - (len(self.stage_6_stack) - self.stage_6_stack_prev)
        self.repeat_bids_day += repeat_bids
        self.repeat_bids.append((self.start_time, repeat_bids))
        # убираем просроченные кредиты
        Monitor.remove_old(self.scoring_stuck_stack, self.last_time, 'scoring_stuck_stack', 'credit_id', 'timestamp')
        # апдейтим количество кредитов зависших на скоринге
        self.scoring_stuck_day.append((self.start_time, len(self.scoring_stuck_stack)))
        if scoring_time:
            logging.debug(f'scoring_time - {scoring_time}')
            last_scoring_time = round(sum(scoring_time) / len(scoring_time), 1)
            if self.scoring_time:
                time = round((sum([i[1] for i in self.scoring_time]) + last_scoring_time) / (len(self.scoring_time) + 1), 1)
                self.scoring_time.append((self.start_time, time))
                logging.debug(f'scoring_time append {time}')
            else:
                self.scoring_time.append((self.start_time, last_scoring_time))
                logging.debug(f'scoring_time append {last_scoring_time}')
        self.approves.append((self.start_time, approves))

    def check_person_stacks(self, persons):
        self.stage_6_stack_prev = len(self.stage_6_stack)
        # добавляем новые заявки
        for person in persons:
            if person['stage'] == 6:
                self.stage_6_stack[person['id']] = person
                logging.debug(f"stage_6_stack append {person}")
                if person['id'] in self.except_6_stack:
                    del self.except_6_stack[person['id']]
            else:
                self.except_6_stack[person['id']] = person
                logging.debug(f"except_6_stack append {person}")
        # убираем просроченные заявки
        Monitor.remove_old(self.stage_6_stack, self.last_time, 'stage_6_stack', 'id', 'create_ts')
        Monitor.remove_old(self.except_6_stack, self.last_time, 'except_6_stack', 'id', 'create_ts')
        # апдейтим количества заявок
        if self.stage_6_stack or self.except_6_stack:
            self.complete_registration_day.append(
                (self.start_time,
                 100 * len(self.stage_6_stack) / (len(self.stage_6_stack) + len(self.except_6_stack)))
            )


if __name__ == '__main__':
    bids = [(datetime.datetime(2020, 5, 3, 17, 58, 16), 3), (datetime.datetime(2020, 5, 3, 18, 3, 16), 4),
            (datetime.datetime(2020, 5, 3, 18, 8, 16), 6), (datetime.datetime(2020, 5, 3, 18, 13, 16), 3),
            (datetime.datetime(2020, 5, 3, 18, 18, 16), 2), (datetime.datetime(2020, 5, 3, 18, 23, 16), 5)]
    credits = [(datetime.datetime(2020, 5, 3, 17, 58, 16), 6), (datetime.datetime(2020, 5, 3, 18, 3, 16), 17),
            (datetime.datetime(2020, 5, 3, 18, 8, 16), 26), (datetime.datetime(2020, 5, 3, 18, 13, 16), 4),
            (datetime.datetime(2020, 5, 3, 18, 18, 16), 7), (datetime.datetime(2020, 5, 3, 18, 23, 16), 8)]
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.set_title("Россия", fontsize=16)
    ax.set_xlabel("Время", fontsize=14)
    ax.grid(which="major", linewidth=1.2)
    ax.grid(which="minor", linestyle="--", color="gray", linewidth=0.5)
    plt.plot([i[0] for i in bids], [i[1] for i in bids], 'o-', label="Cреднее время скоринга в минутах")
    plt.plot([i[0] for i in credits], [i[1] for i in credits],'o-', label="credits")
    ax.legend()
    ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.yaxis.set_minor_locator(AutoMinorLocator())
    ax.tick_params(which='major', length=10, width=2)
    ax.tick_params(which='minor', length=5, width=1)
    plt.show()