from template.table import Table, Record
from template.index import Index
from template.query import Query
import sys
import threading

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self, i):
        self.queries = []
        self.exclusive_locks = {}  # key:key, value:True/False
        self.shared_locks = {}  # key:key, value:True/False
        self.table = None
        self.num = i
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, key, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        # self.lock.acquire()
        # delete = 1 (key)
        # insert = num cols (key, value, value...)
        # update = num cols + 1 (key, None, None,value, ....)
        # select  = 3 (key, col, [1,1,1,1,1])
        # ag/sum = 3 (start, end, col)
        print("running thread ", self.num)
        for query, args in self.queries:
            query_object = query.__self__
            table = query_object.table
            self.table = table
            # checking for exclusive lock first
            if len(args) != 3:
                # write actions
                # get key
                key = args[0]
                # check if we already have a shared lock
                has_shared = False
                if key in self.shared_locks and self.shared_locks[key] == True:
                    has_shared = True
                # check if we already has an exclusive lock
                if key in self.exclusive_locks and self.exclusive_locks[key] == True:
                    continue
                #check for lock_checker_exclusive
                self.exclusive_locks[key] = self.table.lock_checker_exclusive(key, has_shared)
                if self.exclusive_locks[key] == False:
                    return self.abort()
                if has_shared:
                    self.shared_locks[key] = False
            elif isinstance(args[2], list):
                # select actions
                # get keys
                if args[1] == 0:
                    # args[0] is primary key
                    key = args[0]
                    # check if we already have an exclusive lock
                    if key in self.exclusive_locks and self.exclusive_locks[key] == True:
                        continue
                    elif key in self.shared_locks and self.shared_locks[key] == True:
                        continue
                    else:
                        self.shared_locks[key] = self.table.lock_checker_shared(key)
                    if self.shared_locks[key] == False:
                        return self.abort()
                else:
                    value = args[0]
                    col = args[1]
                    key_list = []
                    rid_list = self.table.index.locate(col, value)
                    if rid_list == False:
                        return self.abort()
                    for rid in rid_list:
                        key_list.append(self.table.get_most_updated(rid)[0])
                    for key in key_list:
                        # check if we already have an exclusive lock
                        if key in self.exclusive_locks and self.exclusive_locks[key] == True:
                            continue
                        elif key in self.shared_locks and self.shared_locks[key] == True:
                            continue
                        else:
                            self.shared_locks[key] = self.table.lock_checker_shared(key)
                        if self.shared_locks[key] == False:
                            return self.abort()
            else:
                # ag/sum (start, end, col)
                start = args[0]
                end = args[1]
                for key in range(start, end):
                    # check if we already have an exclusive lock
                    if key in self.exclusive_locks and self.exclusive_locks[key] == True:
                        continue
                    elif key in self.shared_locks and self.shared_locks[key] == True:
                        continue
                    else:
                        self.shared_locks[key] = self.table.lock_checker_shared(key)
                    if self.shared_locks[key] == False:
                        return self.abort()
        # somehow need when the transaction to start running
        for query, args in self.queries:
            # assigning lock to this transactions
            # have a list of keys that are in this transaction --> shared lock
            # upgrade the ones that are gonna be read to exclusive lock
            # this includes removing these keys from the shared lock lists
            # lock + lock check
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                # we need to remove everything happened after that time stamp
                return self.abort()
        print(self.table.index_directory)
        print(self.table.page_directory)
        return self.commit()

    def abort(self):
        # TODO: do roll-back and any other necessary operations
        # remove data that was put in after that time stamp
        # the whole transaction gets abort
        for key in self.exclusive_locks:
            if self.exclusive_locks[key] == True:
                self.table.unlock_exclusive(key)
        for key in self.shared_locks:
            if self.shared_locks[key] == True:
                self.table.unlock_shared(key)
        return False

    def commit(self):
        # TODO: commit to database
        # unlock here
        for key in self.exclusive_locks:
            if self.exclusive_locks[key] == True:
                self.table.unlock_exclusive(key)
        for key in self.shared_locks:
            if self.shared_locks[key] == True:
                self.table.unlock_shared(key)
        # self.lock.release()
        return True
