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
    # cound each repeated value as unique in RID in order to search for the indirection columns.
    Used in: find_most_updated()
    """
    def find_unique(rids,rid):
        start_at = -1
        indices = []
        while True:
            try:
                index = rids.index(rid,start_at+1)
            except ValueError:
                break
            else:
                indices.append(index)
                start_at = index
        return indices
    
    
    """
    # This is the hopping function
    find bae page, then hop to the tail page which has Indirection = None
    Return the index in the tail pages at which the most updated version exists
    Used in: Select() and Update()
    """
    def find_most_updated(self, rid):
        offset = self.table.page_directory[rid][1]
        indirection = self.table.page[self.table.num_columns - 1][offset] # the indirection in base page
        if not indirection:
            return (offset, False) # offset, and in the base pages
        else:
            # Update in the tail page
            # convert bytearray to list in order to find unique
            rids = list(self.page.tail_page)
            indices = find_unique(rids, rid)
            # find the tail page indirection columns
            indirection = self.table.page.tail_page[self.table.num_columns - 1]
            # note that if our indirection is pointing to None, it will return error since result must be integer
            # we want to find the index for None
            counter = 0
            while tail_ind != None:
                for i in indices: # we find all indices with same RID
                    tail_ind = indirection[i] # change index
                counter += 1 # count the index so we know where the None is from the indices
                return index #** please double check but it supposed to eventually return the correct index
            offset = indices[counter] # the offset in the tail page
            return (offset, True)
            
    """
    # Check if the data exist or not 
    Used in: Delete() and Sum()
    """

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, key):
        # ** flag the most updated one + flag the base page
        # OR don't need to find the most updated one (deletion in base page only)
        # >> note that this means the tail page is still using the space
        # >> if we want complete deletion, we would need to access both base AND tail page
        # 
        # set the base page indirection column to -1 
        # and when the user access using the key,
        # indirection point to -1 return no data found == successful deletion
        # for the data exsit, maybe anther function would do (since summation also need to check)
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

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, key, column, query_columns):
        #** might have to add an if-else statment to check if the record is locked
        rids = self.table.index.locate(column, key) # return a list of RIDs
        record = [] # return the list of record
        for rid in rids:
            page_directory = self.table.page_directory[rid] # map to the base page and the offset
            pageID = page_directory[0]
            offset = find_most_updated(rid) # here return the offset we are looking at
            # if the offset is in base page
            if in_tail_page is False: #** edit!!
                record_in_byte = self.table.page[pageID].data[offset] # record is in bytearray!!
            # the offset is in tail page
            else:
                record_in_byte = self.table.page[pageID].tail_page[offset] # record is in bytearray!!
        # convert the record into readible form
        for i in range(len(record_in_byte)-4): # delete the internal columns
            record[i] = int.from_bytes(record_in_byte[(i)*8:(i+1)*8],byteorder = 'big')
        return record
            

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
    def summation(self, start_range, end_range, aggregate_column_index):
        # use locate range return a list of RIDs
        rids = self.table.index.locate_range(start_range, end_range, aggregate_column_index)
        # check if all rids exist in the page directory
        keys = []
        for k in range(start_range, end_range):
            keys += [k]
        return keys
        summation = []
        # check each RIDs
        for rid in rids:
            # if one of them does not exist
            # ** can switch this part to a new function
            page_directory = self.table.page_directory[rid]
            if len(page_directory) == 0:
                return False
            else:
                # use the select function (not sure the input)
                for key in keys:
                    #** not sure about aggregation
                    #** also not sure about the query columns in this case
                    summation = [a+b for a, b in zip(summation, select(key, aggregate_column_index, query_columns))
                return summation
                
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
