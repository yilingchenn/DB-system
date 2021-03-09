class Planner:

    def __init__(self, transaction_worker):
        self.transaction_worker = transaction_worker
        self.transaction_list = self.transaction_worker.transactions
        # High priority is a list where each element corresponds to a page range. Each priority list has 5 queues for
        # a total of 10 queues.
        self.high_priority = [None] * 5
        self.low_priority = [None] * 5
        self.execution_threads = 2
        self.planning_threads = 2

    # Put each of the transactions and individual query in each transaction into the priority queues
    def plan(self):
        # Iterate over each transaction in the transaction worker
        for i in range(0, len(self.transaction_list)):
            # Iterate over all the queries in the transaction
            query_list = self.transaction_list[i].queries
            for j in range(0, len(query_list)):
                # each element in transaction.queries is a tuple: the query object like query.insert and the arguments
                query_object = query_list[j][0].__self__
                query_arguments = query_list[j][1]
                # Extract the different read/write requests and put them into high_priority and low_priority

    # GO through high priority and low priority lists and execute the queries in them
    def execute(self):



