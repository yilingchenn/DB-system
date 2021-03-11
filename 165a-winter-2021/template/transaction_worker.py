
from template.table import Table, Record
from template.index import Index
from template.planner import Planner
import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self):
        self.stats = []
        self.transactions = []
        self.result = 0
        self.thread = threading.Thread(target=self.start_thread, args=())
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    # Starts the thread by calling planner.
    def start_thread(self):
        planner = Planner(self)
        planner.plan()
        self.stats = planner.execute()
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

    """
    Runs a transaction
    """
    def run(self):
        # Calls start_thread --> Planner, execute, etc..
        self.thread.start()
