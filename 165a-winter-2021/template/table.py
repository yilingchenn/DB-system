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
        self.index = Index(self) # index object
        # rid_counter keeps track of the current rid to avoid duplicates
        self.rid_counter = 0
        # num_pages keeps track of the pageID we're currently adding to. Initially, this is one.
        self.num_page = 2
        # Put all of the config constants into one variable
        self.config = init()
        # base_pages_internal is a list of page Id's that belong to the internal pages of a base record
        # base_page_external is a list of page Id's that belong to the external pages of a base record
        self.base_pages_internal = [1]
        self.base_pages_external = [2]
        # tail_pages is a list of pageId's that belong to tail pages.
        self.tail_pages = [0]
        # Every table in the database has access to the shared bufferpool object
        self.bufferpool = bufferpool
        # Implementing locks
        self.shared_locks = {}
        self.exclusive_locks = {}

    # Creates a new file corresponding to the page_id to write/read from
    def create_new_file(self, page_id, table_name, num_cols):
        path = self.bufferpool.path
        # Specify the file name
        file = str(table_name) + '_' + str(page_id) + '.txt'
        pages = []
        for i in range(0, num_cols):
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
    def get_current_page_id_internal(self):
        return self.base_pages_internal[len(self.base_pages_internal)-1]

    def get_current_page_id_external(self):
        return self.base_pages_external[len(self.base_pages_external)-1]

    # Given a page ID, the name of the table, and whether the page is base/tail and internal/external,
    # return the bufferpool slot with the data in the list of pages. This function also automatically loads it
    # into the bufferpool.
    def return_bufferpool_slot(self, page_id, table_name, is_external, is_tail):
        if self.bufferpool.index_of(table_name, page_id) == -1 and is_tail == True:
            # Read a tail page that isn't already in the bufferpool
            bufferpool_slot = self.bufferpool.read_file(page_id, self.name, self.total_columns)
        else:
            if self.bufferpool.index_of(table_name, page_id) == -1 and is_external == True:
                # Read a base page external that isn't already in the bufferpool
                bufferpool_slot = self.bufferpool.read_file(page_id, self.name, self.num_columns)
            elif self.bufferpool.index_of(table_name, page_id) == -1 and is_external == False:
                # Read a base page internal that isn't already in the bufferpool
                bufferpool_slot = self.bufferpool.read_file(page_id, self.name, 4)
            else:
                # Page already in the bufferpool, so need to move it to the front and return it
                slot_index = self.bufferpool.index_of(self.name, page_id)
                bufferpool_slot = self.bufferpool.slots[slot_index]
                self.bufferpool.move_to_front(slot_index)
        return bufferpool_slot


    # From the key of the base page, return an array that represents all the contents of the record that has the key
    # Returns a list that represents the
    def get_most_updated(self, rid):
        # Get all information from the base record
        # rid = self.index_directory[key]
        base_page_id_internal = self.page_directory[rid][0]
        base_page_id_external = self.page_directory[rid][1]
        base_offset = self.page_directory[rid][2]
        # Read the base page into the bufferpool
        bufferpool_slot_base_internal = self.return_bufferpool_slot(base_page_id_internal, self.name, False, False)
        bufferpool_slot_base_external = self.return_bufferpool_slot(base_page_id_external, self.name, True, False)
        # Get indirection and schema encoding from bufferpool slot
        indirection = self.get_indirection_base(bufferpool_slot_base_internal, base_offset)
        schema_encoding = self.get_schema_encoding_base(bufferpool_slot_base_internal, base_offset)
        lineage = bufferpool_slot_base_external.pages[0].lineage
        if indirection != self.config.max_int and lineage < indirection:
            # We have a tail page, so need to get the tail_offset and tail_pageId from page_directory using
            # indirection of base page as key
            tail_page_id = self.page_directory[indirection][0]
            tail_offset = self.page_directory[indirection][2]
            bufferpool_slot_tail = self.return_bufferpool_slot(tail_page_id, self.name, False, True)
            tail_record = []
            for i in range(0, len(bufferpool_slot_tail.pages)-4):
                tail_record.append(self.get_record_element(bufferpool_slot_tail, tail_offset, i))
            # If the "most updated" points to all None, then it means it has been deleted
            deleted = [self.config.max_int] * self.num_columns
            if tail_record == deleted:
                return deleted
        # Read the values to get the most updated values
        record_as_list = []
        num_col = self.num_columns
        for i in range(0, num_col):
            if schema_encoding[i] == '0':
                # Read from base page
                element = self.get_record_element(bufferpool_slot_base_external, base_offset, i)
            elif schema_encoding[i] == '1' and lineage >= indirection:
                # Read from base page, because it has been merged already
                element = self.get_record_element(bufferpool_slot_base_external, base_offset, i)
            else:
                # Read from the tail
                element = self.get_record_element(bufferpool_slot_tail, tail_offset, i)
            record_as_list.append(element)
        return record_as_list

    # Checker takes in the bufferpool slots that corresponds to the current base page.
    # Checker checks if it is full, and if it is, then it allocates another base page, loads that empty base page
    # into the bufferpool, and returns the corresponding bufferpool slot.
    def checker(self, bufferpool_slot_internal, bufferpool_slot_external):
        # Check the capacity of the current bufferpool slot.
        # If its full, we need to allocate a new base page/tail page, create the files
        # Because the new base page/tail pages are guaranteed to be empty, we can just create empty slots.
        pages = bufferpool_slot_internal.pages
        if not pages[0].has_capacity():
            # Add one to the page_range and allocate files for the new internal and external pages.
            self.num_page += 1
            self.base_pages_internal.append(self.num_page)
            self.create_new_file(self.get_current_page_id_internal(), self.name, 4)
            bufferpool_slot_internal = self.bufferpool.read_file(self.num_page, self.name, 4)
            self.num_page += 1
            self.base_pages_external.append(self.num_page)
            self.create_new_file(self.get_current_page_id_external(), self.name, self.num_columns)
            bufferpool_slot_external = self.bufferpool.read_file(self.num_page, self.name, self.num_columns)
            # Check to see if we are in a new Page Range, and allocate empty tail page if we are.
            if len(self.base_pages_internal) % self.config.page_range_size == 1:
                self.num_page += 1
                self.tail_pages.append(self.num_page)
                self.create_new_file(self.num_page, self.name, self.total_columns)
        return bufferpool_slot_internal, bufferpool_slot_external

    # Given the pageId of a base page, return the pageId corresponding to the tail page for that base page.
    # Checks if the tail page has capacity, and merges if it doesn't.
    # Return the slot in the bufferpool.
    def get_tail_page(self, base_page_id_internal):
        base_index = self.base_pages_internal.index(base_page_id_internal)
        range_size = self.config.page_range_size
        tail_index = base_index//range_size
        tail_page_id = self.tail_pages[tail_index]
        if tail_page_id == 0:
            # Tail page has not been allocated yet. Need to allocate it, we know we will have space becasue its empty.
            self.num_page += 1
            self.tail_pages[tail_index] = self.num_page
            self.create_new_file(self.num_page, self.name, self.total_columns)
            return self.num_page
        else:
            tail_page_slot = self.return_bufferpool_slot(tail_page_id, self.name, False, True)
            tail_page_slot_pages = tail_page_slot.pages
            if not tail_page_slot_pages[0].has_capacity():
                # Current bufferpool doesn't have capacity, so need to merge.
                self.merge(tail_index)
                self.num_page += 1
                self.tail_pages[tail_index] = self.num_page
                self.create_new_file(self.num_page, self.name, self.total_columns)
                tail_page_id = self.num_page
            return tail_page_id

    def range_number_to_base_id(self, range_number):
        base_page_id_array = []
        start = range_number * self.config.page_range_size
        end = (range_number + 1) * self.config.page_range_size
        for i in range(start, end):
            if i < len(self.base_pages_internal):
                base_page_id_array.append(i)
        return base_page_id_array

    # Merge occurs fully in the background
    def merge(self, range_number):
        # Using the range number, get all of indexes of the id's of the base internal/external pages that belong to the
        # the range.
        base_page_id_list = self.range_number_to_base_id(range_number)
        bufferpool_object_base_list_internal = []
        bufferpool_object_base_list_external = []
        lineage = -1
        # Load in bufferpool slot objects
        for i in range(0, len(base_page_id_list)):
            base_page_id_internal = self.base_pages_internal[base_page_id_list[i]]
            base_page_id_external = self.base_pages_external[base_page_id_list[i]]
            bufferpool_object_base_list_internal.append(self.return_bufferpool_slot(base_page_id_internal, self.name, False, False))
            bufferpool_object_base_list_external.append(self.return_bufferpool_slot(base_page_id_external, self.name, True, False))
        # Create new pages for merge that will replace base pages.
        new_bufferpool_object_base_list_external = []
        indexes_list = []
        for i in range(0, len(bufferpool_object_base_list_external)):
            self.num_page += 1
            self.create_new_file(self.num_page, self.name, self.num_columns)
            new_slot = self.return_bufferpool_slot(self.num_page, self.name, True, False)
            new_bufferpool_object_base_list_external.append(new_slot)
            indexes_list.append(self.num_page)
        for i in range(0, len(bufferpool_object_base_list_internal)):
            bufferpool_object_base_internal = bufferpool_object_base_list_internal[i]
            bufferpool_object_base_external = bufferpool_object_base_list_external[i]
            bufferpool_object_base_pages_key = bufferpool_object_base_external.pages[0]
            num_records = bufferpool_object_base_pages_key.num_records
            new_bufferpool_object_base_external = new_bufferpool_object_base_list_external[i]
            for j in range(0, num_records):
                # j is the offset.
                rid = self.get_record_element(bufferpool_object_base_internal, j, 0)
                # rid = self.index_directory[key]
                most_updated_external_cols = self.get_most_updated(rid)
                self.write_external_columns(new_bufferpool_object_base_external, most_updated_external_cols)
                # get indirection of base page record
                indirection_base = self.get_indirection_base(bufferpool_object_base_internal, j)
                if indirection_base > lineage and indirection_base != self.config.max_int:
                    lineage = indirection_base
                # rid = self.index_directory[key]
                self.page_directory[rid][1] = indexes_list[i]
            self.base_pages_external[base_page_id_list[i]] = indexes_list[i]
            new_bufferpool_object_base_external.is_clean = False
        # Set lineage for all objects in bufferpool object
        for i in range(0, len(bufferpool_object_base_list_external)):
            bufferpool_object_base = bufferpool_object_base_list_external[i]
            bufferpool_object_pages_external = bufferpool_object_base.pages
            bufferpool_object_pages_internal = bufferpool_object_base_list_internal[i].pages
            for j in range(0, len(bufferpool_object_pages_external)):
                bufferpool_object_pages_external[j].lineage = lineage
            for k in range(0, len(bufferpool_object_pages_internal)):
                bufferpool_object_pages_internal[k].lineage = lineage

    # Writes new external cols to the bufferpool object.
    def write_external_columns(self, bufferpool_object, external_cols):
        pages = bufferpool_object.pages
        for i in range(0, len(pages)):
            page = pages[i]
            element = external_cols[i]
            page.write(element)

    # Returns the indirection of the base page located at pageID, offset.
    def get_indirection_base(self, bufferpool_object, offset):
        pages = bufferpool_object.pages
        page = pages[3]
        current_indirection = page.read(offset)
        current_indirection_decoded = int.from_bytes(current_indirection, byteorder="big")
        return current_indirection_decoded

    # Set the indirection of a base page located at  pageId, offset to an updated value.
    def set_indirection_base(self, bufferpool_object, offset, new_indirection):
        pages = bufferpool_object.pages
        page = pages[3]
        page.edit(offset, new_indirection)

    # Get the schema encoding from a base page located at pageID, offset
    def get_schema_encoding_base(self, bufferpool_object, offset):
        pages = bufferpool_object.pages
        page = pages[2]
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
        page = pages[2]
        int_schema_encoding = int(new_schema_encoding)
        page.edit(offset, int_schema_encoding)

    # Set the schema_encoding of a base page located at  pageId, offset to an updated value.
    def set_timestamp_base(self, bufferpool_object, offset, timestamp):
        pages = bufferpool_object.pages
        page = pages[1]
        page.edit(offset, timestamp)

    # Given the pageId, offset, and col (0, num_cols), return the most individual element of a record in base page
    def get_record_element(self, bufferpool_object, offset, col):
        pages = bufferpool_object.pages
        page = pages[col]
        element = page.read(offset)
        element_decoded = int.from_bytes(element, byteorder = "big")
        return element_decoded

    # list of lists is converted to a single list
    def flatten_page_directory_list(self, page_directory_values):
        new_list = []
        for i in range(0, len(page_directory_values)):
            list_element = page_directory_values[i]
            for j in range(0, len(list_element)):
                new_list.append(list_element[j])
        return new_list

    # Takes in any kind of list and returns in bytes.
    def list_values_to_bytes(self, list_values):
        byte_array_length = len(list_values) * 8
        byte_array = bytearray(byte_array_length)
        for i in range(0, len(list_values)):
            element_bytes = list_values[i].to_bytes(8, byteorder="big")
            start = i*8
            end = (i+1)*8
            byte_array[start:end] = element_bytes
        return byte_array

    def save_table(self):
        path = self.bufferpool.path
        file_name = self.name + "_directory.txt"
        with open(os.path.join(path, file_name), 'wb') as ff:
            """
            Writes the following in order to save and reopen database 
            1.) RID Counter, also the length of the page directory
            2.) Page directory keys 
            3.) Page directory values [1, 2, 3, ...] where 1, 2, 3 belong to page_directory keys [0]
            4.) Length of the index directory
            5.) Index directory keys
            6.) Index directory values
            7.) Number of columns
            8.) Name 
            9.) Key
            10.) Length of base_page_internal and base_page_external (the same)
            11.) Number of pages. 
            """
            # First value is the length of the page directory which is RID counter
            rid_counter_bytes = self.rid_counter.to_bytes(8, byteorder="big")
            ff.write(rid_counter_bytes)
            # Write page directory keys and values
            page_directory_keys = list(self.page_directory.keys())
            page_directory_keys_bytes = self.list_values_to_bytes(page_directory_keys)
            ff.write(page_directory_keys_bytes)
            page_directory_values = list(self.page_directory.values())
            page_directory_values_flattened = self.flatten_page_directory_list(page_directory_values)
            page_directory_values_flattened_bytes = self.list_values_to_bytes(page_directory_values_flattened)
            ff.write(page_directory_values_flattened_bytes)
            # Second hard coded value is the length of the index_directory
            index_directory_length = len(self.index_directory.keys())
            index_directory_length_bytes = index_directory_length.to_bytes(8, byteorder="big")
            ff.write(index_directory_length_bytes)
            # Write index_directory keys and values
            index_directory_keys = list(self.index_directory.keys())
            index_directory_keys_bytes = self.list_values_to_bytes(index_directory_keys)
            ff.write(index_directory_keys_bytes)
            index_directory_values = list(self.index_directory.values())
            index_directory_values_bytes = self.list_values_to_bytes(index_directory_values)
            ff.write(index_directory_values_bytes)
            # Write the num columns
            num_columns_bytes = self.num_columns.to_bytes(8, byteorder='big')
            ff.write(num_columns_bytes)
            # Write the key
            key_bytes = self.key.to_bytes(8, byteorder="big")
            ff.write(key_bytes)
            # Write the length of the base internal/external --> the same
            base_pages_internal_length = len(self.base_pages_internal)
            base_pages_internal_length_bytes = base_pages_internal_length.to_bytes(8, byteorder="big")
            ff.write(base_pages_internal_length_bytes)
            base_pages_internal_to_bytes = self.list_values_to_bytes(self.base_pages_internal)
            ff.write(base_pages_internal_to_bytes)
            base_pages_external_to_bytes = self.list_values_to_bytes(self.base_pages_external)
            ff.write(base_pages_external_to_bytes)
            # Write the length of the tail page and the tail page array
            tail_pages_length = len(self.tail_pages)
            tail_pages_length_bytes = tail_pages_length.to_bytes(8, byteorder="big")
            ff.write(tail_pages_length_bytes)
            tail_pages_to_bytes = self.list_values_to_bytes(self.tail_pages)
            ff.write(tail_pages_to_bytes)
            # Write the number of pages
            num_pages_bytes = self.num_page.to_bytes(8, byteorder="big")
            ff.write(num_pages_bytes)

    # def put_shared_lock(self, key):
    #     # Put a shared lock on the key by incrementing the corresponding value in self.shared_locks to
    #     # indicate that another transaction is reading that key
    #     if key not in self.shared_locks:
    #         self.shared_locks[key] = 1
    #     else:
    #         self.shared_locks[key] += 1

    # def put_exclusive_lock(self, key):
    #     # Put a exclusive lock on the key by setting the corresponding value in self.shared_locks to TRUE
    #     self.exclusive_locks[key] = True

    # def unlock_shared(self, key):
    #     # Put a shared lock on the key by decrementing the corresponding value in self.shared_locks to
    #     # indicate that one transaction is finished reading that key
    #     self.shared_locks[key] -= 1

    # def unlock_exclusive(self, key):
    #     # Unlock exclusive lock on the key by setting the corresponding value in self.shared_locks to FALSE
    #     self.exclusive_locks[key] = False

    # def lock_checker_exclusive(self, key, has_shared):
    #     # case 1: self.shared_locks doesnt exist
    #             # pass/put exclusive
    #     # case 2: self.shared_locks does exist and is 0
    #             # pass/ exclusive
    #     # case 3: self.shared_locks does exist and is 1
    #         # a: if this transaction has the shared_lock
    #             # pass/ exlcusive
    #         # b: if this transaction does not have the shared_lock
    #             # returns False
    #     # case 4: self.shared_locks does exist and is greater than 1
    #             # return False
    #     if key not in self.shared_locks or self.shared_locks[key] == 0:
    #         pass
    #     elif self.shared_locks[key] == 1 and has_shared:
    #         self.shared_locks[key] = 0
    #         pass
    #     else:
    #         return False

    #     # check for exclusive
    #     if key not in self.exclusive_locks:
    #         # nobody has it and it has been generated
    #         self.put_exclusive_lock(key)
    #         return True
    #     elif self.exclusive_locks[key] == True:
    #         # Already have an exclusive lock
    #         return False
    #     else:
    #         self.put_exclusive_lock(key)
    #         return True

    # def lock_checker_shared(self, key):
    #     if key not in self.shared_locks:
    #         self.put_shared_lock(key)
    #         return True
    #     elif self.shared_locks[key] >= 0:
    #         self.put_shared_lock(key)
    #         return True
    #     else:
    #         return False

