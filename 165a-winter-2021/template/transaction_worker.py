from template.table import Table, Record
from template.index import Index
from template.planner import Planner

class TransactionWorker(Planner):

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs a transaction
    """
    def run(self):
        # each transaction returns True if committed or False if aborted
        if len(self.transactions) > 1: # if there are two or more transactions to process
            self.transactions.plan_high_priority() # plan one to high priority
            self.transactions.plan_low_priority() # plan one to low priority
        elif len(self.transactions) == 1: # if there is only one transaction left to process
            self.transactions.plan_high_priority() # put into high priority
        else: # no more transactions
            pass           
        
        while not self.high_priority.empty(): # while the high priority group is not empty
            self.stats.append(self.high_priority.run_high_priortity()) # execute high priority group queries

        # once high priority group is empty, run low priority group queries
        while not self.low_priority.empty():
            self.stats.append(self.low_priority.run_low_priortity())

        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))