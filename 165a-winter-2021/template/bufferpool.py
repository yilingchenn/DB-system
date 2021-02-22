# from template.table import Table, Record
from template.page import Page
# from index import Index
# from time import time
from template.config import init

import os

class Slot:

    def __init__(self, table_name, page_id, pages):
        self.is_clean = True  # Represents whether it has been updated or not
        self.table_name = table_name  # Key that identifies the table that the pages came from
        self.page_id = page_id  # Within the table, the pageID that the memory in the slot corresponds to
        self.pages = pages  # The actual list of page objects representing the data


class Bufferpool:

    def __init__(self):
        self.path = "" # Path represents the path to the folder where all files are. Empty initially.
        self.slots = [] # 16 slots for 16 base pages/tail pages of different tables in the database
        self.config = init() # Config file with all the hard coded variables
        for i in range(0, len(self.slots)):
            slot = Slot()
            self.slots.append(slot)

    # Set the path
    def set_path(self, path):
        self.path = path

    # Evict a slot from the bufferpool by writing it to files and then popping it off.
    def evict_least_used(self):
        slot_object = self.slots[len(self.slots)-1]
        self.write_file(slot_object)
        self.slots.pop(len(self.slots)-1)

    # To keep the least used at the back, we want to move every slot we're updating to the front
    def move_to_front(self, i):
        slot = self.slots[i]
        self.slots.pop(i)
        self.slots.insert(0, slot)

    # Returns the index of a specified base/tail page with page_id that belongs to a table with the key
    def index_of(self, table_key, page_id):
        for i in range(0, len(self.slots)):
            if self.slots[i].table_name == table_key and self.slots[i].page_id == page_id:
                return i
        return -1

    # Write the slot object to files
    # Should we evict when we
    def write_file(self, slot_object):
        if not slot_object.is_clean:
            path = self.path
            pages = slot_object.pages
            # Specify the file name
            file = str(slot_object.table_name) + '_' + str(slot_object.page_id) + '.txt'
            # Creating a file at specified location
            with open(os.path.join(path, file), 'wb') as ff:
                # Write num_records to the first line
                num_records = pages[0].num_records
                num_records_bytes = num_records.to_bytes(8, byteorder='big')
                ff.write(num_records_bytes)
                # Write lineage to the second line
                lineage = pages[0].lineage
                lineage_bytes = lineage.to_bytes(8, byteorder='big')
                ff.write(lineage_bytes)
                # Write remaining byte arrays
                for i in range(0, len(pages)):
                    ff.write(pages[i].data)

    # Assuming that file with page_id and table_name exists already, we access that base page in the table and read it
    # into a bufferpool slot, and then return that bufferpool slot.
    def read_file(self, page_id, table_name, num_columns):
        # 1.) Read a file with specified pageID, tableName
        path = self.path
        file = str(table_name) + '_' + str(page_id) + '.txt'
        with open(os.path.join(path, file), 'rb') as ff:
            f = ff.read()
            # 2.) Put information in file into a list of page objects
            # Create list of empty pages --> will fill
            pages = []
            for i in range(0, num_columns):
                new_page = Page()
                pages.append(new_page)
            # The first 8 bytes of every file contain the number of records. Extract the number of records first.
            num_records = int.from_bytes(f[0:8], byteorder='big')
            # The second 8 bytes of every file contain the lineage of all the pages. Extract the lineage
            lineage = int.from_bytes(f[8:16], byteorder='big')
            offset = 2
            for j in range(0, len(pages)):
                page = pages[j]
                for k in range(0, int(self.config.page_size/8)):
                    if k < num_records:
                        start = offset * 8
                        end = (offset + 1) * 8
                        element = int.from_bytes(f[start:end], byteorder='big')
                        page.write(element)
                    offset += 1
        # 3) Create slot object with page information
        new_slot = Slot(table_name, page_id, pages)
        # 4.) Put slot object into bufferpool, evicting pages that haven't been used the longest.
        if len(self.slots) == self.config.bufferpool_size:
            self.evict_least_used()
        self.slots.insert(0, new_slot)
        # 5.) Return slot object.
        return new_slot

    # When the database is closed, we want to write everything in the Bufferpool and evict it.
    def write_all(self):
        slots = self.slots
        for i in range(0, len(slots)):
            slot_object = slots[i]
            self.write_file(slot_object)
        for i in range(0, len(slots)):
            self.evict_least_used()






