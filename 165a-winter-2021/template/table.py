from template.page import *
from template.index import Index
from time import time

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        # Total columns = num_columns + 4 internal columns (RID, Indirection, Schema, Timestamp)
        self.total_columns = num_columns + 4
        self.num_columns = num_columns
        self.page_directory = {} #{RID: (page, column, offset)}
        self.index_directory = {} # {Key: RID}
        self.index = Index(self) # Not sure what to do with this right now
        # Need to iterate through and allocate a new page every time to avoid changing all pages
        self.page = []
        # whenever we create a page, no matter tail or base
        for i in range(0, self.total_columns):
            new_page = Page()
            self.page.append(new_page)
        self.rid_counter = 0
        self.num_page = 1
        self.config = init()
        self.base_page = [1]
        self.tail_page = []

        # generate RID
    def gen_rid(self):
        self.rid_counter += 1
        return self.rid_counter

    # If one of the pages does not have capacity, then all pages won't have capacity,
    # and another page range needs to be added to account for more pages.
    def checker(self):
        # tail = [3,7]
        # base = [1,2,4,5,6,8]
        # Check the capacity of the current page range
        if not self.page[(self.base_page[len(self.base_page)-1] - 1) * self.total_columns].has_capacity():
            # Allocate new pages
            for i in range(0, self.total_columns):
                new_page = Page()
                self.page.append(new_page)
            # Add one to the page_range
            self.num_page += 1
            self.base_page.append(self.num_page)
            # for every page_range_size
        if len(self.base_page) % self.config.page_range_size == 1:
            # you create an index for the tail page
            self.tail_page.append(0)

    def get_tail_page(self, base_pageId):
        base_index = self.base_page.index(base_pageId) # 2
        range_size = self.config.page_range_size # 3
        tail_index = base_index//range_size # 0
        if self.tail_page[tail_index] == 0 or not self.page[(self.tail_page[tail_index] - 1) * self.total_columns].has_capacity():
            for i in range(0, self.total_columns):
                new_page = Page()
                self.page.append(new_page)
            self.num_page += 1
            self.tail_page[tail_index] = self.num_page
        return self.tail_page[tail_index]

    # Don't have to implement for this cycle
    def __merge(self):
        pass

    # Returns the indirection of the base page located at pageID, offset.
    def get_indirection_base(self, pageId, offset):
        indirection_index = self.total_columns - 1
        page_index = ((pageId - 1) * self.total_columns) + indirection_index
        page = self.page[page_index]
        current_indirection = page.read(offset)
        current_indirection_decoded = int.from_bytes(current_indirection, byteorder="big")
        return current_indirection_decoded

    # Set the indirection of a base page located at  pageId, offset to an updated value.
    def set_indirection_base(self, pageId, offset, new_indirection):
        indirection_index = self.total_columns - 1
        page_index = ((pageId - 1) * self.total_columns) + indirection_index
        page = self.page[page_index]
        page.edit(offset, new_indirection)

    # Get the schema encoding from a base page located at pageID, offset
    def get_schema_encoding_base(self, pageID, offset):
        schema_encoding_index = self.total_columns - 2
        page_index = ((pageID - 1) * self.total_columns) + schema_encoding_index
        page = self.page[page_index]
        schema_encoding = page.read(offset)
        int_schema_encoding = int.from_bytes(schema_encoding, byteorder = "big")
        str_schema_encoding = str(int_schema_encoding)
        # for loop to add back beginning zeros in schema encoding
        for i in range(self.num_columns-len(str_schema_encoding)):
            str_schema_encoding = "0" + str_schema_encoding
        return str_schema_encoding

    # Set the schema_encoding of a base page located at  pageId, offset to an updated value.
    def set_schema_encoding_base(self, pageId, offset, new_schema_encoding):
        schema_encoding_index = self.total_columns - 2
        page_index = ((pageId-1) * self.total_columns) + schema_encoding_index
        page = self.page[page_index]
        int_schema_encoding = int(new_schema_encoding)
        page.edit(offset, int_schema_encoding)

    # Given the pageId, offset, and col (0, num_cols), return the most individual element of a record in base page
    def get_record_element(self, pageId, offset, col):
        page_index = ((pageId-1) * self.total_columns) + col
        page = self.page[page_index]
        element = page.read(offset)
        element_decoded = int.from_bytes(element, byteorder = "big")
        return element_decoded

    """
    Helper function. Takes in the index at which to insert a record into a column
    from (0-total_columns-1), and the function Returns the appropriate page
    index to insert the record.
    """
    def return_appropriate_index(self, page_range, index):
        return ((page_range - 1) * self.total_columns) + index