from template.table import Table, Record
from template.index import Index
#from template.transaction_worker import TransactionWorker

class Planner:

    def __init__(self, transaction_worker):
        self.transaction_worker = transaction_worker
        self.transaction_list = self.transaction_worker.transactions
        # High priority is a list where each element corresponds to a page range. Each priority list has 5 queues for
        # a total of 10 queues.
        self.high_priority = [[None] for i in range(5)]
        self.low_priority = [[None] for i in range(5)]
        self.execution_threads = 2
        self.planning_threads = 2

    # Put each of the transactions and individual query in each transaction into the priority queues
    def plan_high_priority(self):
        # take the first transaction in the transaction worker
        query_list = self.transaction_list[0].queries
        for j in range(0, len(query_list)):
            if j % 5 == 0: # append to first queue
                self.high_priority[0].append(query_list[j])
            elif j % 5 == 1: # append to second queue
                self.high_priority[1].append(query_list[j])
            elif j % 5 == 2: # append to third queue
                self.high_priority[2].append(query_list[j])
            elif j % 5 == 3: # append to fourth queue
                self.high_priority[3].append(query_list[j])
            else: # append to fifth queue
                self.high_priority[4].append(query_list[j])
        return self.high_priority
  
    def plan_low_priority(self):
        # take the second transaction in the transaciton worker
        query_list = self.transaction_list[1].queries
        for j in range(0, len(query_list)):
            if j % 5 == 0: # append to sixth queue
                self.low_priority[0].append(query_list[j])
            elif j % 5 == 1: # append to seventh queue
                self.low_priority[1].append(query_list[j])
            elif j % 5 == 2: # append to eighth queue
                self.low_priority[2].append(query_list[j])
            elif j % 5 == 3: # append to ninth queue
                self.low_priority[3].append(query_list[j])
            else: # append to tenth queue
                self.low_priority[4].append(query_list[j])
        return self.low_priority