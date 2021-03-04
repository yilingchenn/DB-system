from template.table import Table, Record
from template.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
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
        print("running whole trans")
        # somehow need when the transaction to start running
        for query, args in self.queries:
            print("running one thing")
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
        return self.commit()

    def abort(self):
        # TODO: do roll-back and any other necessary operations
        # remove data that was put in after that time stamp
        # the whole transaction gets abort
        return False

    def commit(self):
        # TODO: commit to database
        # unlock here
        return True
