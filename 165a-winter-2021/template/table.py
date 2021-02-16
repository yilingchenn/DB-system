from template.page import *
from template.index import Index
from time import time
from template.bufferpool import Bufferpool
import os

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
        # rid_counter keeps track of the current rid to avoid duplicates
        self.rid_counter = 0
        # num_pages keeps track of the pageID we're currently adding to. Initially, this is one.
        self.num_page = 1
        # Put all of the config constants into one variable
        self.config = init()
        # base_pages is the list of pageId's that correspond to base pages.
        # len(base_pages) is the number of base pages, while the last is always the one inserting to.
        self.base_pages = [1]
        # tail_pages is a list of pageId's that belong to tail pages.
        self.tail_pages = []
        # Every table in the database has access to the shared bufferpool object
        self.bufferpool = bufferpool
        # Open first file for first base page.
        self.create_new_file(1, self.name)

    # Creates a new file corresponding to the page_id to write/read from
    def create_new_file(self, page_id, table_name):
        path = self.bufferpool.path
        # Specify the file name
        file = str(table_name) + '_' + str(page_id) + '.txt'
        pages = []
        for i in range(0, self.total_columns):
            new_page = Page()
            pages.append(new_page)
        with open(os.path.join(path, file), 'wb') as ff:
            # Write 0, no records
            num_records = 0
            num_records_bytes = num_records.to_bytes(8, byteorder='big')
            ff.write(num_records_bytes)
            for i in range(0, len(pages)):
                ff.write(pages[i].data)

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

    def get_most_updated(self, key, column, query_columns):
        record_list = []
        # Get all information from the base record
        rid = self.table.index_directory[key]
        base_pageId = self.page_directory[rid][0]
        base_offset = self.page_directory[rid][1]
        bufferpool_slot_base = self.bufferpool.read_file(base_pageId, self.name, self.total_columns)
        indirection = self.get_indirection_base(bufferpool_slot_base, base_pageId, base_offset)
        schema_encoding = self.get_schema_encoding_base(base_pageId, base_offset)
        if indirection != self.config.max_int:
            # We have a tail page, so need to get the tail_offset and tail_pageId from page_directory using
            # indirection of base page as key
            tail_pageId = self.page_directory[indirection][0]
            tail_offset = self.page_directory[indirection][1]
            bufferpool_slot_tail = self.bufferpool.read_file(tail_pageId, self.name, self.total_columns)
        # Read the values to get the most updated values
        num_col = self.num_columns
        for i in range(0, num_col):
            if schema_encoding[i] == '0':
                # Read from base page
                element = self.get_record_element(bufferpool_slot_base, base_offset, i)
            else:
                # Read from tail page
                element = self.get_record_element(bufferpool_slot_tail, tail_offset, i)
            list_element = element * query_columns[i]
            record_list.append(list_element)
        record = Record(key, rid, record_list)
        return record

    def checker(self, bufferpool_slot):
        # Check the capacity of the current bufferpool slot.
        # If its full, we need to allocate a new base page/tail page, create the files
        # Because the new base page/tail pages are guaranteed to be empty, we can just create empty slots.
        pages = bufferpool_slot.pages
        if not pages[0].has_capacity():
            # Add one to the page_range and allocate files
            self.num_page += 1
            self.base_pages.append(self.num_page)
            # Need to evict the full pages from bufferpool, which are at the front
            self.bufferpool.evict_front()
            # Create the new file for new base page.
            self.create_new_file(self.get_current_page_id(), self.name)
            self.bufferpool.read_file(self.get_current_page_id(), self.name, self.total_columns)
        # If we allocated another base page, need to put in a tail page. However, because we don't have any updates yet,
        # we don't increment the number of pages.
        if len(self.base_pages) % self.config.page_range_size == 1:
            # you allocate a page_id to tail page.
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
        tail_page_id = self.tail_pages[tail_index]
        if tail_page_id == 0:
            # No updates yet, need to allocate a pageID for the tail page.
            self.num_page += 1
            self.tail_pages[tail_index] = self.num_page
            self.create_new_file(self.num_page, self.name)
            return self.tail_pages[tail_index]
        else:
            # We have a tail page, need to check if there is capacity.
            bufferpool_slot = self.bufferpool.read_file(tail_page_id, self.name, self.total_columns)
            pages = bufferpool_slot.pages
            if not pages[0].has_capacity:
                # Current tail page doesn't have capacity, need to create another file.
                # TODO: Call merge function here! We have the tail pages in bufferpool already.
                self.num_page += 1
                self.tail_pages[tail_index] = self.num_page
                self.create_new_file(self.num_page, self.name)
            return self.tail_pages[tail_index]


    # TODO: Implement Merge for Milestone 2
    # Merge occurs fully in the background
    def __merge__(self, base_pageId):

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
    def get_indirection_base(self, bufferpool_object, offset):
        pages = bufferpool_object.pages
        indirection_index = self.total_columns - 1
        page = pages[indirection_index]
        current_indirection = page.read(offset)
        current_indirection_decoded = int.from_bytes(current_indirection, byteorder="big")
        return current_indirection_decoded

    # Set the indirection of a base page located at  pageId, offset to an updated value.
    def set_indirection_base(self, bufferpool_object, offset, new_indirection):
        pages = bufferpool_object.pages
        indirection_index = self.total_columns - 1
        page = pages[indirection_index]
        page.edit(offset, new_indirection)

    # Get the schema encoding from a base page located at pageID, offset
    def get_schema_encoding_base(self, bufferpool_object, offset):
        pages = bufferpool_object.pages
        schema_encoding_index = self.total_columns - 2
        page = pages[schema_encoding_index]
        schema_encoding = page.read(offset)
        int_schema_encoding = int.from_bytes(schema_encoding, byteorder = "big")
        str_schema_encoding = str(int_schema_encoding)
        # for loop to add back beginning zeros in schema encoding
        for i in range(self.num_columns-len(str_schema_encoding)):
            str_schema_encoding = "0" + str_schema_encoding
        return str_schema_encoding

    # Set the schema_encoding of a base page located at  pageId, offset to an updated value.
    def set_schema_encoding_base(self, bufferpool_object, offset, new_schema_encoding):
        pages = bufferpool_object.pages
        schema_encoding_index = self.total_columns - 2
        page = pages[schema_encoding_index]
        int_schema_encoding = int(new_schema_encoding)
        page.edit(offset, int_schema_encoding)

    # Given the pageId, offset, and col (0, num_cols), return the most individual element of a record in base page
    def get_record_element(self, bufferpool_object, offset, col):
        pages = bufferpool_object.pages
        page = pages[col]
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
