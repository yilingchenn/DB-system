
from template.table import Table, Record
from template.index import Index
from template.planner import Planner

class TransactionWorker:

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
        planner = Planner(self)
        planner.plan()
        self.stats = planner.execute()
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))