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
            while tail_ind != 0: # None type conversion
                for i in indices: # we find all indices with same RID
                    tail_ind = indirection[i] # change index
                counter += 1 # count the index so we know where the None is from the indices
                return index #** please double check but it supposed to eventually return the correct index
            offset = indices[counter] # the offset in the tail page
            return (offset, True)
            
    """
    # Check if the data exist or not 
    Used in: Delete() and Sum() and Update()
    """

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, key):
        # check if key exist, return False if it does not
        # else: continue
        # locate using key and pick a random columns = 0
        old_rid = self.table.index.locate(0, key) # return the RID in base page
        # use the page range to find partilar page to delete
        base_page_range = self.table.page_directory[old_rid][0]
        base_offset = self.table.page_directory[old_rid][1]
        # columns = [None] * (num_columns - num_internal_columns)
        columns = [None] * (num_columns - 4)
        # generate internal columns for tail page
        new_rid = self.table.gen_rid()
        timestamp = int(round(time() * 1000))
        schema_encoding = '0' * (num_columns - 4)
        schema_encoding = schema_encoding.encode()
        # indirection = old_rid
        indirection = old_rid
        # combine them and write_tail_page
        new_value = columns + [new_rid, timestamp, schema_encoding, indirection]
        # use write_tail_page
        for i in range(len(new_value)):
            self.table.page[(base_page_range-1)*self.table.num_columns+(i+1)].write_tail_page(new_values[i])
        # set base page RID indirection to a new tail page RID
        self.table.page[base_page_range*(self.table.num_columns - 3)].replace_base_page(indirection, base_offset)
        return True

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
        timestamp = int(round(time() * 1000))
        schema_encoding = '0' * (num_columns - 4) # eliminate internal columns
        schema_encoding = schema_encoding.encode()
        indirection = 0 # How to convert none into byte?
        # using less place
        # the checker checks if a page is full and +1 for page range if full
        self.table.checker()
        page_range = self.table.page_range
        offsets = self.table.page[num_columns - 1].num_records
        self.table.page_directory[rid]= page_range, offsets
        for i in range(num_columns-4):
            self.table.page[i].write_base_page(columns[i])
        # Put in internal records
        self.table.page[num_columns - 4].write_base_page(rid)
        self.table.page[num_columns - 3].write_base_page(timestamp)
        self.table.page[num_columns - 2].write_base_page(schema_encoding)
        self.table.page[num_columns - 1].write_base_page(indirection)
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
            offset, tail_page = find_most_updated(rid) # here return the offset we are looking at
            if tail_page is False: # if the offset is in base page
                record_in_byte = self.table.page[pageID].data[offset]
            else: # the offset is in tail page
                record_in_byte = self.table.page[pageID].tail_page[offset]
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
        # check if the given key exist, return False if doesn't exist
        # else: continue
        
        # generate new RID
        new_rid = self.table.gen_rid()
        # check the most updated
        old_rid = self.table.index.locate(0, key) # locate in the base page
        base_page_range = self.table.page_directory[old_rid][0]
        base_offset = self.table.page_directory[old_rid][1]
        indirection = old_rid
        # check most update, if it's the offset in base page, second value is False
        offset, tail_page = find_most_updated(self, old_rid)
        # if the most updated version is in the base_page
        new_time = int(round(time() * 1000))
        new_schema_encoding = ""
        # return the most updated
        old_record = self.select(key, 0, [1]*len(columns)) #including the internal ones
        if tail_page == False:
            # compare only
            for i in range(len(old_record)):
                if old_record[i] == columns[i]:
                    new_schema_encoding[i] = "0"
                else:
                    new_schema_encoding[i] = "1"
            # write into the tail_page for the new update
        else:
            old_schema_encoding = self.table.page[base_page_range*(self.table.num_columns - 2)].data[base_offset]
            old_schema_encoding = schema_encoding.decode() # convert back to string
            if old_record[i] == columns[i]:
                new_schema_encoding[i] = "0"
            else:
                new_schema_encoding[i] = "1"
            for i in range(len(new_schema_encoding)):
                if old_schema_encoding[i] == "1":
                    new_schema_encoding[i] = "1"
        
        # now let's say we figure out the schema encoding
        for i in range(len(schema_encoding)):
            columns[i] = columns[i] * int(schema_encoding[i])
        # combine columns + internal columns
        new_values = columns + [new_rid, new_time, schema_encoding, indirection]
        for i in range(self.table.num_columns):
            # we append to the tail page
            # num_columns is the total columns
            self.table.page[(base_page_range-1)*self.table.num_columns+(i+1)].write_tail_page(new_values[i])
        # now we change the internal column values in the base page[schema_encoding, indirection]
        new_schema_encoding = schema_encoding.encode()
        self.table.page[base_page_range*(self.table.num_columns - 2)].replace_base_page(new_schema_encoding, base_offset)
        self.table.page[base_page_range*(self.table.num_columns - 3)].replace_base_page(indirection, base_offset)
        return True

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
        if len(rids) == 0:
            return False
        summation = 0
        # use the select function (not sure the input)
        for rid in rids:
            page_range = self.table.page_directory[rid][0]
            offset = self.table.page_directory[rid][1]
            summation = summation + self.table.page[page_range*(aggregate_column_index+1)].data[offset]
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
