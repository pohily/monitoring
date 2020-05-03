import datetime
from decimal import Decimal
from collections import deque
import matplotlib.pyplot as plt

from constants import TIME_DELTA, STACK_DURATION


class Monitor():
    def __init__(self, start_time=None):
        self.complete_bids_day = []         # Текущее количество заявок в стадии 6 за сутки (STACK_DURATION)
        self.incomplete_bids_day = []       # Текущее количество заявок в стадии < 6 за сутки (STACK_DURATION)
        self.inapproved_credits_day = []    # Текущее количество кредитов в статусах -1, 0 за сутки (STACK_DURATION)
        self.complete_registration_day = [] # Текущий % прохождения цепочки за сутки (STACK_DURATION)
        self.scoring_stuck_day = []         # Текущее количество кредитов зависших на скоринге за сутки (STACK_DURATION)
        ############## stacks
        self.stage_6_stack = deque()        # persons on stage 6 за сутки (STACK_DURATION)
        self.except_6_stack = deque()       # persons on stage < 6 за сутки (STACK_DURATION)
        self.scoring_stuck_stack = deque()  # credits with statuses 0 за сутки (STACK_DURATION)
        self.ids_stack = set()              # set айдишников из scoring_stuck_stack
        ############## metrics
        self.new_bids = []                  # Количество новых заявок за TIME_DELTA
        self.approves = []                  # Количество одобрений за TIME_DELTA
        self.amount = []                    # сумма выданных кредитов за TIME_DELTA
        self.pastdue = []                   # количство уходов в просрочку за TIME_DELTA
        self.pastdue_repayment = []         # количество выхода из просрочки за TIME_DELTA
        self.scoring_time = []              # среднее время скоринга за TIME_DELTA
        ############## под вопросом
        self.repeate_bids = []              # Количество повторных заявок за TIME_DELTA
        self.partner_bids = []              # количество заявок через партнеров за TIME_DELTA

        NOW = datetime.datetime.now()
        if not start_time:
            self.start_time = NOW - datetime.timedelta(minutes=TIME_DELTA)
            self.last_time = NOW
        else:
            self.start_time = start_time
            if not self.last_time:
                self.last_time = NOW
            else:
                self.start_time = self.last_time
                self.last_time = self.last_time + datetime.timedelta(minutes=TIME_DELTA)

    def find_metrics(self, persons, statuses):
        self.new_bids.append((self.last_time, len(persons)))

        amount, pastdue, pastdue_repayment, approves, scoring_time = 0, 0, 0, 0, []
        for status in statuses:
            if status['to'] == 4:
                pastdue += 1
            if (status['from'] == 4 and status['to'] == 2) or (status['from'] == 4 and status['to'] == 3):
                pastdue_repayment += 1
            # проверка одобрений
            if status['from'] == 1 and status['to'] == 2:
                approves += 1
                if status['credit_id'] in self.ids_stack:
                    self.ids_stack.remove(status['credit_id'])
                    for credit in self.scoring_stuck_stack:
                        if credit['id'] == status['credit_id']:
                            scoring_time.append(status['timestamp'] - credit['create_ts'])
                            amount += credit['amount']
                            break
        # todo convert to seconds?
        self.scoring_time.append((self.last_time, sum(scoring_time) / len(scoring_time)))
        self.approves.append((self.last_time, approves))
        self.pastdue.append((self.last_time, pastdue))
        self.pastdue_repayment.append((self.last_time, pastdue_repayment))
        self.amount.append((self.last_time, amount))

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
        while self.last_time - self.stage_6_stack[0]['create_ts'] > datetime.timedelta(hours=STACK_DURATION):
            self.stage_6_stack.popleft()
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
        while self.last_time - self.scoring_stuck_stack[0]['create_ts'] > datetime.timedelta(hours=STACK_DURATION):
            credit = self.scoring_stuck_stack.popleft()
            self.ids_stack.remove(credit['id'])
        # апдейтим количество кредитов зависших на скоринге
        self.scoring_stuck_day.append((self.last_time, len(self.scoring_stuck_stack)))

    def draw_graphs(self):
        pass