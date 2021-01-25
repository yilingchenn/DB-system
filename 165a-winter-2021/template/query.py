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
    # Delete a record by updating everything to null and making the RID -1
    def delete(self, key):
        update(key, [None]*self.table.num_columns)
        self.table.index_map[key] = -1


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason --> Not implemented,
    #   Why would it fail (?)
    """
    def insert(self, *columns):
        # Step 1: Create a new record with the specified information, using the
        # first element of columns as the key and put it in pages[0]
        new_RID = self.table.generate_RID()
        new_record = Record(new_RID, columns[0], self.table.num_columns)
        self.table.pages[0].write_base_page(new_record)
        # Step 2: Insert the remainder of the record into the base pages
        for i in range(1, len(columns)):
            self.table.pages[i].write_base_page(columns[i])
        # Step 3: Update the index_map and page_directory
        self.table.index_map[columns[0]] = new_RID
        # The location of the newest records correspond to the length-1 of the
        # base pages, because we appended them to the end.
        self.table.page_directory[new_RID] = self.table.pages[0].return_base_length()-1
        return True

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    # Question: What does the column parameter represent??
    # Question: How to return a list of record objects when they all come from
    # The same record because they have the same key??
    # Eg: [1234, 45, 67, 89] query_columns = [0, 1, 1, 1], so return 45, 67, 89?
    def select(self, key, column, query_columns):
        pass

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking

    Eg: If current record looks like this [1234, 23, 15, 37] and the update looks
    like this --> [None, 24, None, 38], then the record changes to [1234, 24, 15, 38]
    Coded right now assuming that the key cannot be changed. (starting at index 1)
    """
    # Need to remember to check the MOST updated version
    def update(self, key, *columns):
        # Step 1: Go through all tail pages and append the new
        new_schema_encoding = ""
        for i in range(1, len(columns)):
            if (columns[i]):
                self.table.pages[i].write_tail_page(columns[i])
                new_schema_encoding += "1"
            else:
                new_schema_encoding += "0"




    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    # How does sum work?
    # Record = [1234, 12, 34, 56]
    # Record = [1235, 78, 90, 100] and start range = 1234 and end range = 1235 and
    # aggregate_columns = 2, then 34 + 90 = 124?
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