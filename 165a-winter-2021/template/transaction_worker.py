from template.table import Table, Record
from template.index import Index
import threading
import time
import queue

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self):
        self.thread = threading.Thread(target = self.run_transaction, args=())
        self.stats = []
        self.transactions = []
        self.result = 0
        self.lock = threading.Lock()
        self.queue = queue.Queue()
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)
    """
    Runs a transaction
    """
    def run_transaction(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            with self.lock:
                self.stats.append(transaction.run())
                # try different time here
                time.sleep(2.5)
        self.result = len(list(filter(lambda x: x, self.stats)))

    def run(self):
        self.thread.start()
        # populate queue with data
        self.lock.acquire()
        for transaction in self.transactions:
            self.queue.put(transaction)
        self.lock.release()
        # wait on the queue until everything has been processed
        # self.queue.join()

