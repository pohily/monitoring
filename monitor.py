import datetime
from decimal import Decimal
from collections import deque

from constants import TIME_DELTA, STACK_DURATION


class Monitor():
    def __init__(self, start_time=None):
        self.complete_bids = []             # Количество заявок в стадии 6 за сутки (STACK_DURATION)
        self.incomplete_bids = []           # Количество заявок в стадии < 6 за сутки (STACK_DURATION)
        self.inapproved_credits = []        # Количество кредитов в статусах -1, 0 за сутки (STACK_DURATION)
        self.complete_registration = []     # % прохождения цепочки за сутки (STACK_DURATION)
        ############
        self.new_bids = []                  # Количество новых заявок за TIME_DELTA
        self.approves = []                  # Количество одобрений за TIME_DELTA
        self.amount = []                    # сумма выданных кредитов за TIME_DELTA
        self.returnsumm = []                # сумма возвращенных кредитов за TIME_DELTA
        self.pastdue = []                   # количство уходов в просрочку за TIME_DELTA
        self.pastdue_repayment = []         # количество выхода из просрочки за TIME_DELTA
        ############## под вопросом
        self.repeate_bids = []              # Количество повторных заявок за TIME_DELTA
        self.scoring_time = []              # среднее время скоринга за TIME_DELTA
        self.partner_bids = []              # количество заявок через партнеров за TIME_DELTA
        ############## stacks
        self.stage_6 = deque()              # persons on stage 6 за сутки (STACK_DURATION)
        self.except_6 = deque()             # persons on stage < 6 за сутки (STACK_DURATION)
        self.not_approved_credits = deque() # credits with statuses -1, 0 за сутки (STACK_DURATION)

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

    def find_metrics(self, credits, persons, statuses):
        self.new_bids.append((self.last_time, len(persons)))

        amount, returnsumm, approves = 0, 0, 0
        for row in credits:
            if row['status'] == 2:
                approves += 1
                amount += row['amount']
            if row['status'] == 3:
                returnsumm += int(row['returnsumm'])
        self.approves.append((self.last_time, approves))
        self.amount.append((self.last_time, amount))
        self.returnsumm.append((self.last_time, returnsumm))

        pastdue, pastdue_repayment = 0, 0
        for row in statuses:
            if row['to'] == 4:
                pastdue += 1
            if (row['from'] == 4 and row['to'] == 2) or (row['from'] == 4 and row['to'] == 3):
                pastdue_repayment += 1

    def check_person_stacks(self, persons):
        # добавляем новые заявки
        for person in persons:
            if person['stage'] == 6:
                self.stage_6.append(person)
                if person in self.except_6:
                    self.except_6.remove(person)
            else:
                self.except_6.append(person)
        # убираем просроченные заявки
        while self.last_time - self.stage_6[0]['create_ts'] > datetime.timedelta(hours=STACK_DURATION):
            self.stage_6.popleft()
        while self.last_time - self.except_6[0]['create_ts'] > datetime.timedelta(hours=STACK_DURATION):
            self.except_6.popleft()
        # апдейтим количества заявок
        self.complete_bids.append((self.last_time, len(self.stage_6)))
        self.incomplete_bids.append((self.last_time, len(self.except_6)))
        self.complete_registration.append(
            (self.last_time, len(self.complete_bids) / (len(self.complete_bids) + len(self.incomplete_bids)))
        )

    def check_credits_stack(self, credits):
        pass

    def draw_graphs(self):
        pass