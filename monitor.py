import datetime
from decimal import Decimal

from constants import TIME_DELTA


class Monitor():
    def __init__(self, start_time=None):
        self.new_bids = []                  # Количество новых заявок
        self.approves = []                  # Количество одобрений
        self.amount = []                    # сумма выданных кредитов
        self.returnsumm = []                # сумма возвращенных кредитов
        self.pastdue = []                   # количство уходов в просрочку
        self.pastdue_repayment = []         # количество выхода из просрочки
        ############## под вопросом
        self.complete_registration = []     # % прохождения цепочки
        self.repeate_bids = []              # Количество повторных заявок
        self.scoring_time = []              # среднее время скоринга
        self.partner_bids = []              # количество заявок через партнеров

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

    def save_stacks(self, persons, credits):
        pass

    def draw_graphs(self):
        pass