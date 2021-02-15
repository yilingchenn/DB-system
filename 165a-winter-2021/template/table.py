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
    def __init__(self, name, num_columns, key, bufferpool):
        self.name = name
        self.key = key
        # Total columns = num_columns + 4 internal columns (RID, Indirection, Schema, Timestamp)
        self.total_columns = num_columns + 4
        self.num_columns = num_columns
        self.page_directory = {} #{RID: (pageId, offset)}
        self.index_directory = {} # {Key: RID}
        self.index = Index(self) # Not sure what to do with this right now
        # Pages is a list of Page Objects, representing all of the database in memory
        self.pages = []
        # Need to allocate a new page object instead of doing [Page()] * total_columns to avoid memory issues
        for i in range(0, self.total_columns):
            new_page = Page()
            self.pages.append(new_page)
        # rid_counter keeps track of the current rid to avoid duplicates
        self.rid_counter = 0
        # num_pages keeps track of the pageID we're currently adding to. Initially, this is one.
        self.num_page = 1
        # Put all of the config constants into one variable
        self.config = init()
        # base_pages is the list of pageId's that correspond to base pages.
        self.base_pages = [1]
        # tail_pages is a list of pageId's that belong to tail pages
        self.tail_pages = []
        # Every table in the database has access to the shared bufferpool object
        self.bufferpool = bufferpool

    # generate RID
    def gen_rid(self):
        self.rid_counter += 1
        return self.rid_counter

    # The last element of the self.base_page array will always correspond to the pageID of
    # our current set of base pages.
    def get_current_page_id(self):
        return self.base_pages[len(self.base_pages)-1]

    # Helper function. Takes in the index at which to insert a record into a column
    # from (0-total_columns-1), and the function Returns the appropriate page
    # index to insert the record.
    def return_appropriate_index(self, page_range, index):
        return ((page_range - 1) * self.total_columns) + index

    def checker(self):
        # Check the capacity of the current base page. We check the first page (at index 0), but this is arbitrary.
        # if one of the pages doesn't have capacity, then all of them won't have capacity.
        if not self.pages[self.return_appropriate_index(self.get_current_page_id(), 0)].has_capacity():
            # Allocate a new base page
            for i in range(0, self.total_columns):
                new_page = Page()
                self.pages.append(new_page)
            # Add one to the page_range
            self.num_page += 1
            self.base_pages.append(self.num_page)
        # If we allocated another base page, need to put in a tail page. However, because we don't have any updates yet,
        # we don't increment the number of pages.
        if len(self.base_pages) % self.config.page_range_size == 1:
            # you create an index for the tail page
            self.tail_pages.append(0)

    # Given the pageId of a base page, return the pageId corresponding to the tail page for that base page.
    def get_tail_page(self, base_pageId):
        base_index = self.base_pages.index(base_pageId)
        range_size = self.config.page_range_size
        # By doing the floor division of the index of the base page ID by the page range size, we can get the
        # index of where the tail page should be
        tail_index = base_index//range_size
        # IF teh tail page Id is 0, there haven't been any updates yet and we need to allocate a new tail page
        # If the page is full, allocate a new tail page
        # TODO: When tail page is full, merge tail page
        if self.tail_pages[tail_index] == 0 or not self.pages[self.return_appropriate_index(self.tail_pages[tail_index] - 1, 0)].has_capacity():
            for i in range(0, self.total_columns):
                new_page = Page()
                self.pages.append(new_page)
            self.num_page += 1
            self.tail_pages[tail_index] = self.num_page
        return self.tail_pages[tail_index]

    # TODO: Implement Merge for Milestone 2
    # Merge occurs fully in the backgroun
    def __merge__(self, base_pageId):
        pass
        """
        # Allocate a new set of pages to eventually be put back into self.pages
        new_pages = []
        for i in range(0, self.table.total_columns):
            new_page = Page()
            new_pages.append(new_page)
        # For each element in the set of base pages, get the most updated using select.
        num_records = self.table.pages[self.table.return_appropriate_index()]
        starting_index = self.table.return_appropriate_index(base_pageId, 0)
        for i in range(starting_index, starting_index + self.table.total_columns):
        """

    # Returns the indirection of the base page located at pageID, offset.
    def get_indirection_base(self, pageId, offset):
        indirection_index = self.total_columns - 1
        page_index = ((pageId - 1) * self.total_columns) + indirection_index
        page = self.pages[page_index]
        current_indirection = page.read(offset)
        current_indirection_decoded = int.from_bytes(current_indirection, byteorder="big")
        return current_indirection_decoded

    # Set the indirection of a base page located at  pageId, offset to an updated value.
    def set_indirection_base(self, pageId, offset, new_indirection):
        indirection_index = self.total_columns - 1
        page_index = ((pageId - 1) * self.total_columns) + indirection_index
        page = self.pages[page_index]
        page.edit(offset, new_indirection)

    # Get the schema encoding from a base page located at pageID, offset
    def get_schema_encoding_base(self, pageID, offset):
        schema_encoding_index = self.total_columns - 2
        page_index = ((pageID - 1) * self.total_columns) + schema_encoding_index
        page = self.pages[page_index]
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
        page = self.pages[page_index]
        int_schema_encoding = int(new_schema_encoding)
        page.edit(offset, int_schema_encoding)

    # Given the pageId, offset, and col (0, num_cols), return the most individual element of a record in base page
    def get_record_element(self, pageId, offset, col):
        page_index = ((pageId-1) * self.total_columns) + col
        page = self.pages[page_index]
        element = page.read(offset)
        element_decoded = int.from_bytes(element, byteorder = "big")
        return element_decoded



"""
To insert:
-Check if the last base page is in the bufferpool already
-If it is in the bufferpool, we can insert using the code we already have! 
-Then write updated pages back to bufferpool
-Mark bufferpool slot as dirty 
-We're done!

BUT if it's not in the bufferpool already
-If the bufferpool is full, we need to make space for it, so evict the pages we've used least recently
-Then we need to get it from the file, put it in the format we understand (list of pages)
-Insert using our existing functions
-Put the updated into bufferpool
-Mark page as dirty
"""

"""
What is the bufferpool? Nested array of pages, where each element is a slot, and each element is a list of pages 
representing a base page. 
[[Page, Page, Page, Page], [Page, Page, Page, Page, Page]]
"""


"""
To Update: *Assuming that tail page already works and we only ever have one tail page per page range*
-Check if the tail page is in the bufferpool.
    -Write it to the bufferpool if its not
-Check if the key maps to a page ID that is already in the bufferpool. 
    -Write it to the bufferpool if its not
-Now, everything is in bufferpool so we can use the syntax that we already have! 
-Mark both pages as dirty
"""


"""
To Merge:
Merge when tail page is full.
-Fetch the entire page range (multiple base pages) and put into bufferpool
-Fetch the tail page and put into bufferpool
-Perform merge function (select most updated, write into new base page)
-Write into bufferpool
"""

"""
bufferpool.create_page(self.pageID, self.name)
--> create a file in the path
"""