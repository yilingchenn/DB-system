class Planner:

    def __init__(self, transaction_worker):
        self.transaction_worker = transaction_worker
        self.transaction_list = self.transaction_worker.transactions
        # high_priority and low_priority are arrays where each element in the array is an execution queue. Nested list.
        self.high_priority = [None] * 10
        self.low_priority = [None] * 10
        # Initialize to empty list
        for i in range(0, 10):
            self.high_priority[i] = []
            self.low_priority[i] = []

    # Given the RID of the read/write, get the slot where it should be in the high/low priority lists
    def get_data_partition(self, rid):
        index = rid%10
        return index

    # Put each of the transactions and individual query in each transaction into the priority queues
    def plan(self):
        # Iterate over each transaction in the transaction worker
        # Initialize current_planning_thread to be zero --> high priority
        current_planning_thread = 0
        for i in range(0, len(self.transaction_list)):
            # Iterate over all the queries in the transaction
            query_list = self.transaction_list[i].queries
            for j in range(0, len(query_list)):
                # each element in transaction.queries is a tuple: the query object like query.insert and the arguments
                query_object = query_list[j][0].__self__
                query_arguments = query_list[j][1]
                # Extract the different read/write requests and put them into high_priority and low_priority
                # ONLY update, insert, and select.
                if len(query_arguments) != 3:
                    # Update and insert
                    if len(query_arguments) == query_object.table.num_columns:
                        # Insert
                        key = query_arguments[0]
                        rid = query_object.table.get_next_rid()
                        index = self.get_data_partition(rid)
                        if current_planning_thread == 0:
                            self.high_priority[index].append((query_list[j][0], query_list[j][1]))
                        else:
                            self.low_priority[index].append((query_list[j][0], query_list[j][1]))
                    else:
                        # Update --> Break up into two functions
                        key = query_arguments[0]
                        rid = query_object.table.index_directory[key]
                        index = self.get_data_partition(rid)
                        if current_planning_thread == 0:
                            self.high_priority[index].append((query_list[j][0], query_list[j][1]))
                        else:
                            self.low_priority[index].append((query_list[j][0], query_list[j][1]))
                else:
                    # select --> assume primary key
                    if query_arguments[1] == 0:
                        key = query_arguments[0]
                        rid = query_object.table.index_directory[key]
                    else:
                        rid = query_object.table.index.locate(query_arguments[1], query_arguments[0])
                        # ^^ might be a list
                        rid = rid[0]
                    # Get the index where it should be in the partition and put it into the priority
                    index = self.get_data_partition(rid)
                    if current_planning_thread == 0:
                        self.high_priority[index].append((query_list[j][0], query_list[j][1]))
                    else:
                        self.low_priority[index].append((query_list[j][0], query_list[j][1]))
                if current_planning_thread == 0:
                    current_planning_thread = 1
                else:
                    current_planning_thread = 0

    # Go through high priority and low priority lists and execute the queries in them
    def execute(self):
        results = []
        # Iterate over 5 slots
        for i in range(0, 10):
            # Iterate over high_priority[i]
            for j in range(0, len(self.high_priority[i])):
                query = self.high_priority[i][j][0]
                args = self.high_priority[i][j][1]
                result = query(*args)
                results.append(True)
            # Iterate over low_priority[i]
            for j in range(0, len(self.low_priority[i])):
                query = self.low_priority[i][j][0]
                args = self.low_priority[i][j][1]
                result = query(*args)
                results.append(True)
        return results

"""
Update --> read(key), write(key)
Insert --> write(key)
Delete --> read(key), write(key)
Select --> read(key)
Aggregate --> read(key), read(key), read(key), read(key)....
Sum --> read(key), read(key), read(key), read(key)....
read(key) == select

Option2 --> When updating, split into two function calls.
read(key) == GetMostUpdated not select
write(key) == Update or Insert depending on the function call.
"""

