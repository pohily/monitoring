import datetime
from decimal import Decimal
from collections import deque
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator

from constants import TIME_DELTA, STACK_DURATION


class Monitor():
    def __init__(self, time_shift=None):
        self.complete_bids_day = []         # Текущее количество заявок в стадии 6 за сутки (STACK_DURATION)
        self.incomplete_bids_day = []       # Текущее количество заявок в стадии < 6 за сутки (STACK_DURATION)
        self.inapproved_credits_day = []    # Текущее количество кредитов в статусах -1, 0 за сутки (STACK_DURATION)
        ############## stacks
        self.stage_6_stack = deque()        # persons on stage 6 за сутки (STACK_DURATION)
        self.except_6_stack = deque()       # persons on stage < 6 за сутки (STACK_DURATION)
        self.scoring_stuck_stack = deque()  # credits with statuses 0 за сутки (STACK_DURATION)
        self.ids_stack = set()              # set айдишников из scoring_stuck_stack
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

        self.NOW = datetime.datetime.now()
        self.start = True       # первый раз данные получаются без задержки
        self.real_time = False # флаг выполненного time_shift
        if time_shift:
            self.start_time = self.NOW - datetime.timedelta(hours=int(time_shift))
            self.last_time = self.start_time + datetime.timedelta(minutes=TIME_DELTA)
        else:
            self.last_time = self.NOW
            self.start_time = self.NOW - datetime.timedelta(minutes=TIME_DELTA)

    def update_time(self):
        self.start_time = self.last_time
        self.last_time = self.last_time + datetime.timedelta(minutes=TIME_DELTA)

    def find_metrics(self, persons, statuses):
        self.new_bids.append((self.last_time, len(persons)))
        approves, scoring_time = 0, []
        for status in statuses:
            if status['from'] == 1 and status['to'] == 2:
                approves += 1
                if status['credit_id'] in self.ids_stack:
                    self.ids_stack.remove(status['credit_id'])
                    for credit in self.scoring_stuck_stack:
                        if credit['id'] == status['credit_id']:
                            # превращаем timedelta в количество минут
                            delta = str(status['timestamp'] - credit['create_ts']).split(':')
                            scoring_time.append(round(int(delta[0])*60 + int(delta[1]) + int(delta[2]) / 60, 1))
                            break
        if scoring_time:
            self.scoring_time.append((self.last_time, round(sum(scoring_time) / len(scoring_time), 1)))
        self.approves.append((self.last_time, approves))

    def check_person_stacks(self, persons):
        # добавляем новые заявки
        for person in persons:
            if person['stage'] == 6:
                self.stage_6_stack.append(person)
                if person in self.except_6_stack:
                    self.except_6_stack.remove(person)
            else:
                self.except_6_stack.append(person)
        # убираем просроченные заявки
        if self.stage_6_stack:
            while self.last_time - self.stage_6_stack[0]['create_ts'] > datetime.timedelta(hours=STACK_DURATION):
                self.stage_6_stack.popleft()
        if self.except_6_stack:
            while self.last_time - self.except_6_stack[0]['create_ts'] > datetime.timedelta(hours=STACK_DURATION):
                self.except_6_stack.popleft()
        # апдейтим количества заявок
        self.complete_bids_day.append((self.last_time, len(self.stage_6_stack)))
        self.incomplete_bids_day.append((self.last_time, len(self.except_6_stack)))
        self.complete_registration_day.append(
            (self.last_time, len(self.stage_6_stack) / (len(self.stage_6_stack) + len(self.except_6_stack)))
        )

    def check_credits_stack(self, credits):
        # добавляем новые кредиты
        for credit in credits:
            if credit['status'] == 2 and credit in self.scoring_stuck_stack:
                self.scoring_stuck_stack.remove(credit)
                self.ids_stack.remove(credit['id'])
            if credit['status'] == 0:
                self.ids_stack.add(credit['id'])
                self.scoring_stuck_stack.append(credit)
        # убираем просроченные кредиты
        if self.scoring_stuck_stack:
            while self.last_time - self.scoring_stuck_stack[0]['create_ts'] > datetime.timedelta(hours=STACK_DURATION):
                credit = self.scoring_stuck_stack.popleft()
                self.ids_stack.remove(credit['id'])
        # апдейтим количество кредитов зависших на скоринге
        self.scoring_stuck_day.append((self.last_time, len(self.scoring_stuck_stack)))

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