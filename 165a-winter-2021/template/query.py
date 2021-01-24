from template.table import Table, Record
from template.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, key):
        pass

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    # *columns gives any function parameters as a tuple:
    #   Can be one element, or can insert many elements at once.
    #   inserted [92113011, 18, 18, 12, 6]
    def insert(self, *columns):
        # Schema encoding is a bit vector with one bit per column that stores
        #   information about the update state of each column. 0 for columns
        #   that have not been updated and 1 for columns that have been updated
        # schema_encoding = '0' * self.table.num_columns
        for column in columns:
            # Create a new record. INDIRECTION_COLUMN = Null because no updated version.
            #   RID = index in the base page ==> some other function we need to write
            #   Timestamp = time at which we process the insertion.
            #   Schema Encoding column = 0, because its not updated, we're first inserting it
            record = Record(rid, key, [null, rid, time, 0])
            # Find the corresponding page, and write the value to the page 

        # Insert into page, insertion function will return the index of the page
        #   where it is located to update the page_directory

        # To insert something:
        #   generate an RID for the new record
        #   append each new Record to the corresponding page

        pass

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, key, column, query_columns):
        pass

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
