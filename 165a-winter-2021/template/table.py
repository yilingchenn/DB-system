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

    # Given a page ID and the name of the table, return the slot for the bufferpool.
    def return_bufferpool_slot(self, page_id, table_name):
        if self.bufferpool.index_of(table_name, page_id) == -1:
            # Base page is not in the bufferpool already, need to load it in
            bufferpool_slot = self.bufferpool.read_file(page_id, self.name, self.total_columns)
        else:
            # Base page already in the bufferpool, need to access it and move it to the front
            slot_index = self.bufferpool.index_of(self.name, page_id)
            bufferpool_slot = self.bufferpool.slots[slot_index]
            self.bufferpool.move_to_front(slot_index)
        return bufferpool_slot


    # From the key of the base page, return an array that represents all the contents of the record that has the key
    # Returns a list that represents the
    def get_most_updated(self, key):
        # Get all information from the base record
        rid = self.index_directory[key]
        base_pageId = self.page_directory[rid][0]
        base_offset = self.page_directory[rid][1]
        # Read the base page into the bufferpool
        bufferpool_slot_base = self.return_bufferpool_slot(base_pageId, self.name)
        # Get indirection and schema encoding from bufferpool slot
        indirection = self.get_indirection_base(bufferpool_slot_base, base_offset)
        schema_encoding = self.get_schema_encoding_base(bufferpool_slot_base, base_offset)
        if indirection != self.config.max_int:
            # We have a tail page, so need to get the tail_offset and tail_pageId from page_directory using
            # indirection of base page as key
            tail_pageId = self.page_directory[indirection][0]
            tail_offset = self.page_directory[indirection][1]
            bufferpool_slot_tail = self.bufferpool.read_file(tail_pageId, self.name, self.total_columns)
        # Read the values to get the most updated values
        record_as_list = []
        num_col = self.num_columns
        for i in range(0, num_col):
            if schema_encoding[i] == '0':
                # Read from base page
                element = self.get_record_element(bufferpool_slot_base, base_offset, i)
            else:
                # Read from tail page
                element = self.get_record_element(bufferpool_slot_tail, tail_offset, i)
            record_as_list.append(element)
        return record_as_list

    # Checker takes in the bufferpool slot that corresponds to the current base page.
    # Checker checks if it is full, and if it is, then it allocates another base page, loads that empty base page
    # into the bufferpool, and returns the corresponding bufferpool slot.
    def checker(self, bufferpool_slot):
        # Check the capacity of the current bufferpool slot.
        # If its full, we need to allocate a new base page/tail page, create the files
        # Because the new base page/tail pages are guaranteed to be empty, we can just create empty slots.
        pages = bufferpool_slot.pages
        if not pages[0].has_capacity():
            # Add one to the page_range and allocate files
            self.num_page += 1
            self.base_pages.append(self.num_page)
            # Create the new file for new base page.
            self.create_new_file(self.get_current_page_id(), self.name)
            # Create a new bufferpool object with the new empty base page.
            bufferpool_slot = self.bufferpool.read_file(self.num_page, self.name, self.total_columns)
            # Check to see if we are in a new Page Range, and allocate empty tail page if we are.
            if len(self.base_pages) % self.config.page_range_size == 1:
                self.num_page += 1
                self.tail_pages.append(self.num_page)
                # Create the new file for new tail page
                self.create_new_file(self.num_page, self.name)
            return bufferpool_slot
        else:
            # Current base page has enough space, so return bufferpool_slot
            return bufferpool_slot

    # Given the pageId of a base page, return the pageId corresponding to the tail page for that base page.
    # Checks if the tail page has capacity, and merges if it doesn't.
    # Return the slot in the bufferpool.
    def get_tail_page(self, base_pageId):
        base_index = self.base_pages.index(base_pageId)
        range_size = self.config.page_range_size
        tail_index = base_index//range_size
        tail_page_id = self.tail_pages[tail_index]
        tail_page_slot = self.bufferpool.read_file(tail_page_id, self.name, self.total_columns)
        tail_page_slot_pages = tail_page_slot.pages
        if not tail_page_slot_pages[0].has_capacity():
            # Current bufferpool doesn't have capacity, so need to merge.
            # TODO: Call merge function here! We have the tail pages in bufferpool already.
            self.merge(tail_index)
            self.num_page += 1
            self.tail_page[tail_index] = self.num_page
            self.create_new_file(self.num_page, self.name)
            tail_page_id = self.num_page
        return tail_page_id

    def range_number_to_base_id(self, range_number):
        base_page_id_array = []
        start = range_number * self.config.page_range_size
        end = (range_number + 1) * self.config.page_range_size
        for i in range(start, end):
            base_page_id_array.append(i)
        return base_page_id_array

    # TODO: Implement Merge for Milestone 2
    # Merge occurs fully in the background
    def merge(self, range_number):
        # Using the range number, get all of the base pages associated with that page range and put them in bufferpool
        base_page_id_list = self.range_number_to_base_id(range_number)
        bufferpool_object_base_list = []
        for i in range(0, len(base_page_id_list)):
            base_page_id = self.base_page[base_page_id_list[i]]
            bufferpool_object_base_list.append(self.bufferpool.read_file(base_page_id, self.name, self.total_columns))
        for i in range(0, len(bufferpool_object_base_list)):
            bufferpool_object_base = bufferpool_object_base_list[i]
            bufferpool_object_base_pages_key = bufferpool_object_base.pages[0]
            num_records = bufferpool_object_base_pages_key.num_records
            for j in range(0, num_records):
                # j is the offset.
                key = self.get_record_element(bufferpool_object_base, j, 0)
                most_updated_external_cols = self.get_most_updated(key)
                schema_encoding_string = '0' * self.num_columns
                schema_encoding = int(schema_encoding_string)
                timestamp = int(round(time() * 1000))
                indirection = self.config.max_int
                self.set_external_columns(bufferpool_object_base, j, most_updated_external_cols)
                self.set_indirection_base(bufferpool_object_base, j, indirection)
                self.set_schema_encoding_base(bufferpool_object_base, j, schema_encoding)
                self.set_timestamp_base(bufferpool_object_base, j, timestamp)
            bufferpool_object_base.is_clean = False

    # Writes new external cols to the bufferpool object.
    def set_external_columns(self, bufferpool_object, offset, external_cols):
        pages = bufferpool_object.pages
        for i in range(0, len(pages)-4):
            page = pages[i]
            element = external_cols[i]
            page.edit(offset, element)

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

    # Set the schema_encoding of a base page located at  pageId, offset to an updated value.
    def set_timestamp_base(self, bufferpool_object, offset, timestamp):
        pages = bufferpool_object.pages
        timestamp_index = self.total_columns - 3
        page = pages[timestamp_index]
        page.edit(offset, timestamp)

    # Given the pageId, offset, and col (0, num_cols), return the most individual element of a record in base page
    def get_record_element(self, bufferpool_object, offset, col):
        pages = bufferpool_object.pages
        page = pages[col]
        element = page.read(offset)
        element_decoded = int.from_bytes(element, byteorder = "big")
        return element_decoded




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
