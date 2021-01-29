from table import Table, Record
from index import Index
from time import time

MAX_INT = 18446744073709551615

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table

    # Returns the indirection of the base page located at pageID, offset.
    def get_indirection_base(self, pageId, offset):
        indirection_index = self.table.total_columns - 1
        page_index = ((pageId - 1) * self.table.total_columns) + indirection_index
        page = self.table.page[page_index]
        current_indirection = page.read_base_page(offset)
        current_indirection_decoded = int.from_bytes(current_indirection, byteorder = "big")
        return current_indirection_decoded

    # Set the indirection of a base page located at  pageId, offset to an updated value.
    def set_indirection_base(self, pageId, offset, new_indirection):
        indirection_index = self.table.total_columns - 1
        page_index = ((pageId - 1) * self.table.total_columns) + indirection_index
        page = self.table.page[page_index]
        page.edit_base_page(offset, new_indirection)

    # Get the schema encoding from a base page located at pageID, offset
    def get_schema_encoding_base(self, pageID, offset):
        schema_encoding_index = self.table.total_columns - 2
        page_index = ((pageID - 1) * self.table.total_columns) + schema_encoding_index
        page = self.table.page[page_index]
        current_schema_encoding = page.read_base_page(offset)
        schema_encoding_decoded =  current_schema_encoding.decode()
        # Schema_encoding is 8 bytes, so need to slice the rest of the bytes off from the decoded string
        schema_encoding_sliced = schema_encoding_decoded[0:self.table.num_columns]
        return schema_encoding_sliced

    # Set the schema_encoding of a base page located at  pageId, offset to an updated value.
    def set_schema_encoding_base(self, pageId, offset, new_schema_encoding):
        schema_encoding_index = self.table.total_columns - 2
        page_index = ((pageId-1) * self.table.total_columns) + schema_encoding_index
        page = self.table.page[page_index]
        page.edit_base_page(offset, new_schema_encoding)

    # Given the pageId, offset, and col (0, num_cols), return the most individual element of a record in base page
    def get_record_element_base(self, pageId, offset, col):
        page_index = ((pageId-1) * self.table.total_columns) + col
        page = self.table.page[page_index]
        element = page.read_base_page(offset)
        element_decoded = int.from_bytes(element, byteorder = "big")
        return element_decoded

    # Given the pageId, offset, and col (0, num_cols), return the most individual element of a record in tail page
    def get_record_element_tail(self, pageId, offset, col):
        page_index = ((pageId - 1) * self.table.total_columns) + col
        page = self.table.page[page_index]
        element = page.read_tail_page(offset)
        element_decoded = int.from_bytes(element, byteorder = "big")
        return element_decoded

    # Returns if the key exists in the dictionary
    def key_exists(self, key):
        return key in self.table.index_directory.keys()

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    # Delete a record by setting the RID to -1
    def delete(self, key):
        # Find the location of the key
        rid = self.table.index_directory[key]
        location = self.table.page_directory[rid]
        # Remove the old RID and replace the key, value pair with RID -1
        self.table.page_directory.pop(rid)
        self.table.page_directory[MAX_INT] = location
        self.table.index_directory[key] = MAX_INT
        # Edit the RID column in the base page
        # RID column is the column at self.total_columns-4
        self.table.page[self.return_appropriate_index(self.table.total_columns-4)].write_base_page(MAX_INT)
        # Make an update that makes all the values of the RID null
        update_array = [MAX_INT] * self.table.num_columns
        self.update(key, update_array)

    """
    Helper function. Takes in the index at which to insert a record into a column
    from (0-total_columns-1), and the function Returns the appropriate page
    index to insert the record.
    """
    def return_appropriate_index(self, page_range, index):
        return ((page_range - 1) * self.table.total_columns) + index

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
        # Indirection is set to the maximum value of an 8 byte
        #   integer --> MAX_INT because there are no updates
        indirection = MAX_INT
        # Check to update page range
        self.table.checker()
        # Map RID to a tuple (page_range, offset)
        offset = self.table.page[(self.table.page_range - 1) * self.table.total_columns].num_records
        page_range = self.table.page_range
        self.table.page_directory[rid] = (page_range, offset)
        # Map key to the RID in the index directory
        self.table.index_directory[columns[0]] = rid
        # Put the columns of the record into the visible columns
        for i in range(0, self.table.total_columns - 4):
            page_index = self.return_appropriate_index(self.table.page_range, i)
            self.table.page[page_index].write_base_page(columns[i])
        # Put the information into the internal records
        self.table.page[self.return_appropriate_index(self.table.page_range, self.table.total_columns - 4)].write_base_page(rid)
        self.table.page[self.return_appropriate_index(self.table.page_range, self.table.total_columns - 3)].write_base_page(timestamp)
        self.table.page[self.return_appropriate_index(self.table.page_range, self.table.total_columns - 2)].write_base_page(schema_encoding_string)
        self.table.page[self.return_appropriate_index(self.table.page_range, self.table.total_columns - 1)].write_base_page(indirection)
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
        # Need to get the columns from the base page and the columns from the tail page and the schema_encoding.
        record_list = []
        rid = self.table.index_directory[key]
        pageId = self.table.page_directory[rid][0]
        offset = self.table.page_directory[rid][1]
        indirection = self.get_indirection_base(pageId, offset)
        schema_encoding = self.get_schema_encoding_base(pageId, offset)
        # Read the values to get the most updated values
        for i in range(0, self.table.num_columns):
            if schema_encoding[i] == '0':
                # Read from base page
                element = self.get_record_element_base(pageId, offset, i)
            else:
                # Read from tail page
                element = self.get_record_element_tail(pageId, offset, i)
            list_element = element * query_columns[i]
            record_list.append(list_element)
        return record_list

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        # Modify columns data to encode MAX_INT as None
        columns = columns[0]
        for i in range(0, len(columns)):
            if columns[i] is None:
                columns[i] = MAX_INT
        # Check if the key even exists in the database
        if not self.key_exists(key):
            return False
        rid = self.table.index_directory[key]
        base_pageId = self.table.page_directory[rid][0]
        base_page_offset = self.table.page_directory[rid][1]
        base_page_indirection = self.get_indirection_base(base_pageId, base_page_offset)
        base_page_schema_encoding = self.get_schema_encoding_base(base_pageId, base_page_offset)
        # Generate values for tail page
        tail_page_rid = self.table.gen_rid()
        timestamp = int(round(time() * 1000))
        most_updated = self.select(key, 0, [1] * self.table.num_columns)
        # Add tail record to page_directory
        offset = self.table.page[(base_pageId - 1) * self.table.total_columns].num_updates
        self.table.page_directory[tail_page_rid] = (base_pageId, offset)
        # Find new schema encoding
        new_schema_encoding = ""
        for i in range(0, len(most_updated)):
            if most_updated[i] == columns[i] or columns[i] == MAX_INT:
                if base_page_schema_encoding[i] == '1':
                    new_schema_encoding += '1'
                else:
                    new_schema_encoding += '0'
            else:
                new_schema_encoding += '1'
        # Find the indirection of the new update
        if base_page_indirection == MAX_INT:
            # Base page is the most updated version, so tail page indirection is base page RID
            tail_page_indirection = rid
        else:
            tail_page_indirection = base_page_indirection
        # Now, write EVERYTHING into tail page since we have all the info we need.
        for i in range(0, len(columns)): # TODO: start with 1 or 0? Can you update keys? Starting with 0 for now
            self.table.page[(base_pageId-1)*self.table.total_columns+i].write_tail_page(columns[i])
        # Write internal columns into tail page
        self.table.page[self.return_appropriate_index(base_pageId, self.table.total_columns - 4)].write_tail_page(tail_page_rid)
        self.table.page[self.return_appropriate_index(base_pageId, self.table.total_columns - 3)].write_tail_page(timestamp)
        self.table.page[self.return_appropriate_index(base_pageId, self.table.total_columns - 2)].write_tail_page(new_schema_encoding)
        self.table.page[self.return_appropriate_index(base_pageId, self.table.total_columns - 1)].write_tail_page(tail_page_indirection)
        # Replace the schema encoding and indirection in the base page
        self.table.page[self.return_appropriate_index(base_pageId, self.table.total_columns - 2)].edit_base_page(base_page_offset, new_schema_encoding)
        self.table.page[self.return_appropriate_index(base_pageId, self.table.total_columns - 1)].edit_base_page(base_page_offset, tail_page_rid)
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
            summation += self.select(key, 0, query_column)[aggregate_column_index]
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
