from template.table import Table, Record
from template.index import Index
from template.planner import Planner

class Transaction(Planner):

    """
    # Creates a transaction object.
    """
    def __init__(self, queries):
        self.queries = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    
    # executes queries in high priority group
    def run_high_priority(self):
        # iterate over queues in high priority group
        for queue in self.high_priority:
            for query, args in queue:
                result = query(*args)
                # If the query has failed the transaction should abort
                if result == False:
                    return self.abort()
            return self.commit()
    
    # executes queries in low priority group
    def run_low_priority(self):   
        # iterate over queues in low priority group 
        for queue in self.low_priority:
            for query, args in queue:
                result = query(*args)
                # If the query has failed the transaction should abort
                if result == False:
                    return self.abort()
            return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False

    def commit(self):
        # TODO: commit to database
        return True