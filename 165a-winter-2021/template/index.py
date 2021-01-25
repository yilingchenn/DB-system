"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
# Index maps Key to RID
class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.indices = [None] *  table.num_columns
        pass

    """
    # returns the location of all records with the given value on column "column"
    # Traverse the specified column, checking if each Record has the value specified in the
    # parameter, and return a list of RID's
    """
    # Returns an RID list. Using that RID list, we can check if the value exists in each of the
    # query columns. Return all the ones that work.
    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    # Traverse the specified column, checking if each record has a value between begin and end
    # and return a list of RID's
    """
    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
