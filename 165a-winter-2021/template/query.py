from template.table import Table, Record
from template.index import Index
from time import time
from template.config import init


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table

    # Returns if the key exists in the dictionary
    def key_exists(self, key):
        if key in self.table.index_directory.keys():
            # if self.select(key, 0, [1]*self.table.num_columns)[0].columns == [MAX_INT]*self.table.num_columns:
            temp = self.select(key, 0, [1]*self.table.num_columns)
            if temp == [self.table.config.max_int]*self.table.num_columns:
                return False
            else:
                return True
        else:
            return False

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    # Delete a record by setting the RID to -1

    def delete(self, key):
        if not self.key_exists(key):
            return False
        # 1. get the base page RID using key
        # 2. set base page schema encoding to '0' *n num_columns
        # 3. use the update function to create a new tail with parameter(key, [None]* num_columns)
        rid = self.table.index_directory[key]
        pageId = self.table.page_directory[rid][0]
        offset = self.table.page_directory[rid][1]
        # overwrite the base page schema_encoding
        bufferpool_object = self.table.bufferpool.read_file(pageId, self.table.name, self.table.total_columns)
        self.table.set_schema_encoding_base(bufferpool_object, offset, '0'*self.table.num_columns)
        # update
        self.update(key, *([None]*self.table.num_columns))
        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # Generate a new RID from table class
        rid = self.table.gen_rid()
        # timestamp for record
        timestamp = int(round(time() * 1000))
        # Schema encoding for internal columns
        schema_encoding_string = '0' * self.table.num_columns
        schema_encoding = int(schema_encoding_string)
        # Indirection is set to the maximum value of an 8 byte
        #   integer --> MAX_INT because there are no updates
        indirection = self.table.config.max_int
        current_base_page = self.table.get_current_page_id()
        bufferpool_slot = self.table.return_bufferpool_slot(current_base_page, self.table.name)
        # Check to update base pages
        updated_bufferpool_slot = self.table.checker(bufferpool_slot)
        # Map RID to a tuple (page_range, offset)
        offset = updated_bufferpool_slot.pages[0].num_records
        base_pageId = self.table.base_pages[len(self.table.base_pages)-1]
        self.table.page_directory[rid] = (base_pageId, offset)
        # Map key to the RID in the index directory
        self.table.index_directory[columns[0]] = rid
        # Put the columns of the record into the visible columns
        for i in range(0, self.table.total_columns - 4):
            updated_bufferpool_slot.pages[i].write(columns[i])
        # Put the information into the internal records
        updated_bufferpool_slot.pages[self.table.total_columns - 4].write(rid)
        updated_bufferpool_slot.pages[self.table.total_columns - 3].write(timestamp)
        updated_bufferpool_slot.pages[self.table.total_columns - 2].write(schema_encoding)
        updated_bufferpool_slot.pages[self.table.total_columns - 1].write(indirection)
        # Mark the bufferpool slot as dirty, because its been updated.
        updated_bufferpool_slot.is_clean = False
        return True

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, key, column, query_columns):
        # Turn select into a helper function in table class and call it here.
        # Need to get the columns from the base page and the columns from the tail page and the schema_encoding.
        columns = self.table.get_most_updated(key)
        for i in range(0, len(query_columns)):
            columns[i] = columns[i] * query_columns[i]
        rid = self.table.index_directory[key]
        record = Record(rid, key, columns)
        return [record]


    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        # Change tuple to list
        col = []
        for i in columns:
            col.append(i)
        # Check if the key even exists in the database
        if not self.key_exists(key):
            return False
        # everything from base page and page_directory
        base_record_rid = self.table.index_directory[key]
        base_pageId = self.table.page_directory[base_record_rid][0] # column
        base_page_offset = self.table.page_directory[base_record_rid][1] # offset
        base_page_indirection = self.table.get_indirection_base(base_pageId, base_page_offset)
        base_page_schema_encoding = self.table.get_schema_encoding_base(base_pageId, base_page_offset)
        # Check to update pages
        self.table.checker()
        # implement the page range
        tail_pageId = self.table.get_tail_page(base_pageId)
        # Generate values for tail page
        tail_page_rid = self.table.gen_rid()
        timestamp = int(round(time() * 1000))
        most_updated = self.select(key, 0, [1] * self.table.num_columns)[0].columns
        # Add tail record to page_directory
        offset = self.table.pages[(tail_pageId - 1) * self.table.total_columns].num_records
        self.table.page_directory[tail_page_rid] = (tail_pageId, offset)
        # Find new schema encoding
        new_schema_encoding = ""
        for i in range(0, len(most_updated)):
            if base_page_schema_encoding[i] == '1' and col[i] is None:
                # Value has been previously updated but is not currently being updated,
                # so need to write the update to the next tail page
                col[i] = most_updated[i]
                new_schema_encoding += "1"
            elif base_page_schema_encoding[i] == '0' and col[i] is not None:
                # Value is currently being updated in this update function, so need to adjust new_schema_encoding
                new_schema_encoding += "1"
            else:
                new_schema_encoding += "0"
        # Find the indirection of the new update
        if base_page_indirection == self.table.config.max_int:
            # Base page is the most updated version, so tail page indirection is base page RID
            tail_page_indirection = base_record_rid
        else:
            tail_page_indirection = base_page_indirection
        # Now, write EVERYTHING into tail page since we have all the info we need.
        for i in range(0, len(col)):
            if col[i] is None:
                col[i] = self.table.config.max_int
                self.table.pages[self.table.return_appropriate_index(tail_pageId, i)].write(col[i])
            else:
                self.table.pages[self.table.return_appropriate_index(tail_pageId, i)].write(col[i])
        # Write internal columns into tail page
        int_schema_encoding = int(new_schema_encoding)
        self.table.pages[self.table.return_appropriate_index(tail_pageId, self.table.total_columns - 4)].write(tail_page_rid)
        self.table.pages[self.table.return_appropriate_index(tail_pageId, self.table.total_columns - 3)].write(timestamp)
        self.table.pages[self.table.return_appropriate_index(tail_pageId, self.table.total_columns - 2)].write(int_schema_encoding)
        self.table.pages[self.table.return_appropriate_index(tail_pageId, self.table.total_columns - 1)].write(tail_page_indirection)
        # Replace the schema encoding and indirection in the base page
        self.table.pages[self.table.return_appropriate_index(base_pageId, self.table.total_columns - 2)].edit(base_page_offset, int_schema_encoding)
        self.table.pages[self.table.return_appropriate_index(base_pageId, self.table.total_columns - 1)].edit(base_page_offset, tail_page_rid)
        return True

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        key_list = []
        for i in range(start_range, end_range + 1):
            if self.key_exists(i):
                key_list.append(i)
        if len(key_list) == 0:
            return False
        summation = 0
        query_column = [0] * self.table.num_columns
        query_column[aggregate_column_index] = 1
        for key in key_list:
            summation += self.select(key, 0, query_column)[0].columns[aggregate_column_index]
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
