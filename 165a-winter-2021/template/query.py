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
        # remove from the page directory and the indices
        # flag the
        pass

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        num_columns = self.table.num_columns
        # Generate all internal column data
        # count how many RID you have and + 1 is the new RID
        rid = self.table.gen_rid()
        # time & schema encoding
        time = time.time().encode()
        schema_encoding = '0' * (num_columns - 4) # eliminate internal columns
        schema_encoding = schema_encoding.encode()
        indirection = None # How to convert none into byte?
        # using less place
        # the checker checks if a page is full and +1 for page range if full
        self.table.checker()
        page_range = self.page_range
        offsets = self.table.page[num_columns - 1].num_records
        self.table.page_directory[rid]= self.table.page[(page_range - 1)*num_columns:page_range*num_column], offsets*num_column
        for i in range(len(columns)):
            self.table.page[i].write_base_page(columns[i])
        # Put in internal records
        self.table.page[len(columns)].write_base_page(rid)
        self.table.page[len(columns) + 1].write_base_page(time)
        self.table.page[len(columns) + 2].write_base_page(schema_encoding)
        self.table.page[len(columns) + 3].write_base_page(indirection)
        return True
        # do we need to check if it never fails anyway?

    def find_most_updated(self, rid):
        # This is the hopping function
        # find bae page, then hop to the tail page which has Indirection = None
        # Return the index in the tail pages at which the most updated version exists
        offset = self.table.page_directory[rid][1]
        indirection = self.table.page[self.table.num_columns - 1][offset]
        if not indirection:
            return (offset, False) # offset, and in the base pages
        else:
            # Update in the tail page
            while (indirection is not None):
                indirection = 
            return (offset, True)


    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, key, column, query_columns):
        rids = self.table.index.locate(column, key) # return a list of RIDs
        record = [] # List of record objects to return
        for rid in rids:
            page_directory = self.table.page_directory[rid] # Get page ID and offset from RID
            pageID = page_directory[0] # page where record is stored
            offset = page_directory[1] # offset
            most_updated = find_most_updated(rid)


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        # cumulative
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
