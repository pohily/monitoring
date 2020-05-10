import datetime
from decimal import Decimal
from collections import deque
import logging

import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator

from constants import TIME_DELTA, STACK_DURATION


class Monitor():
    def __init__(self, time_shift=None, country=None):
        self.complete_bids_day = []         # Текущее количество заявок в стадии 6 за сутки (STACK_DURATION)
        self.incomplete_bids_day = []       # Текущее количество заявок в стадии < 6 за сутки (STACK_DURATION)
        self.inapproved_credits_day = []    # Текущее количество кредитов в статусах -1, 0 за сутки (STACK_DURATION)
        self.approves_day = 0               # Текущее общее количество одобрений за сутки (STACK_DURATION)
        self.new_bids_day = 0               # Текущее общее количество новых заявок за сутки (STACK_DURATION)
        self.repeat_bids_day = 0            # Текущее общее количество повторных заявок за сутки (STACK_DURATION)
        self.total_bids_day = 0             # Текущее общее количество заявок за сутки (STACK_DURATION)
        ############## stacks
        self.stage_6_stack = deque()        # persons on stage 6 за сутки (STACK_DURATION)
        self.stage_6_stack_prev = 0         # persons on stage 6 за сутки (STACK_DURATION) в предыдущий период
        self.except_6_stack = deque()       # persons on stage < 6 за сутки (STACK_DURATION)
        self.scoring_stuck_stack = {}       # credits stuck in scoring
        ############## metrics
        self.complete_registration_day = [] # Текущий % прохождения цепочки за сутки (STACK_DURATION)
        self.scoring_stuck_day = []         # Текущее количество кредитов зависших на скоринге за сутки (STACK_DURATION)
        self.new_bids = []                  # Количество новых заявок за TIME_DELTA
        self.approves = []                  # Количество одобрений за TIME_DELTA
        self.scoring_time = []              # среднее время скоринга за TIME_DELTA - в минутах
        ############## под вопросом
        #self.pastdue = []                   # Количество уходов в просрочку за TIME_DELTA
        #self.pastdue_repayment = []         # Количество выхода из просрочки за TIME_DELTA
        #self.repeate_bids = []              # Количество повторных заявок за TIME_DELTA
        #self.partner_bids = []              # количество заявок через партнеров за TIME_DELTA
        
        if not country:
            self.country = 'Россия'
        else:
            self.country = country
        self.NOW = datetime.datetime.now()
        self.time_shift = None
        self.start = True       # первый раз данные получаются без задержки
        self.real_time = False # флаг выполненного time_shift
        if time_shift:
            self.time_shift = int(time_shift)
            self.start_time = self.NOW - datetime.timedelta(hours=self.time_shift)
            self.last_time = self.start_time + datetime.timedelta(minutes=TIME_DELTA)
        else:
            self.last_time = self.NOW
            self.start_time = self.NOW - datetime.timedelta(minutes=TIME_DELTA)

    def update_time(self):
        self.start_time = self.last_time
        self.last_time = self.last_time + datetime.timedelta(minutes=TIME_DELTA)

    def find_metrics(self, persons, statuses):
        self.new_bids.append((self.start_time, len(persons)))
        self.new_bids_day += len(persons)
        approves, scoring_time = 0, []
        total_bids_day_prev = self.total_bids_day
        for status in statuses:
            # credit goes to scoring
            if status['from'] == 0 and status['to'] == 0:
                self.total_bids_day += 1
                self.scoring_stuck_stack[status['credit_id']] = status
                logging.debug(f"append {status['credit_id']} to scoring_stuck_stack")
            # approve fails after scoring
            if status['from'] == 0 and status['to'] == 100:
                if status['credit_id'] in self.scoring_stuck_stack:
                    del self.scoring_stuck_stack[status['credit_id']]
                    logging.debug(f"Remove {status['credit_id']} from scoring_stuck_stack")
            # approves
            if status['from'] == 1 and status['to'] == 2:
                approves += 1
                self.approves_day += 1
                # scoring_time
                if status['credit_id'] in self.scoring_stuck_stack:
                    # превращаем timedelta в количество минут
                    delta = str(status['timestamp'] - self.scoring_stuck_stack[status['credit_id']]['timestamp']).split(':')
                    scoring_time.append(round(int(delta[0]) * 60 + int(delta[1]) + int(delta[2]) / 60, 1))
                    del self.scoring_stuck_stack[status['credit_id']]
                    logging.debug(f"Remove {status['credit_id']} from scoring_stuck_stack")

        self.repeat_bids_day += self.total_bids_day - total_bids_day_prev - (len(self.stage_6_stack) - self.stage_6_stack_prev)
        # убираем просроченные кредиты
        to_del = []
        if self.scoring_stuck_stack:
            for key, value in self.scoring_stuck_stack.items():
                if datetime.datetime.now() - value['timestamp'] > datetime.timedelta(hours=STACK_DURATION):
                    logging.debug(f"Remove {value['credit_id']} from scoring_stuck_stack")
                    to_del.append(key)
            for key in to_del:
                del self.scoring_stuck_stack[key]
        # апдейтим количество кредитов зависших на скоринге
        self.scoring_stuck_day.append((self.start_time, len(self.scoring_stuck_stack)))
        if scoring_time:
            last_scoring_time = sum(scoring_time) / len(scoring_time)
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
                self.stage_6_stack.append(person)
                logging.debug(f"stage_6_stack.append {person}")
                if person in self.except_6_stack:
                    self.except_6_stack.remove(person)
            else:
                self.except_6_stack.append(person)
                logging.debug(f"except_6_stack.append {person}")
        # убираем просроченные заявки
        if self.stage_6_stack:
            while datetime.datetime.now() - self.stage_6_stack[0]['create_ts'] > datetime.timedelta(hours=STACK_DURATION):
                x = self.stage_6_stack.popleft()
                logging.debug(f"Remove {x} from stage_6_stack")
        if self.except_6_stack:
            while datetime.datetime.now() - self.except_6_stack[0]['create_ts'] > datetime.timedelta(hours=STACK_DURATION):
                x = self.except_6_stack.popleft()
                logging.debug(f"Remove {x} from except_6_stack")
        # апдейтим количества заявок
        self.complete_bids_day.append((self.start_time, len(self.stage_6_stack)))
        self.incomplete_bids_day.append((self.start_time, len(self.except_6_stack)))
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