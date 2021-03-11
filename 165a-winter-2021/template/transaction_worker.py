from template.table import Table, Record
from template.index import Index
import threading
import time

class TransactionWorker (threading.Thread):

    """
    # Creates a transaction worker object.
    """
    def __init__(self):
        threading.Thread.__init__(self)
        self.stats = []
        self.transactions = []
        self.result = 0
        self.lock = threading.Lock()
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
        self.lock.acquire()
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
            time.sleep(1)
        # for False in stats
        # put transaction into queue
        # loop until queue is empty
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
        self.lock.release()
        time.sleep(1)