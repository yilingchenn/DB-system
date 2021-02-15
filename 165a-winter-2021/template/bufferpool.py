from template.table import Table, Record
from template.page import Page
from template.index import Index
from time import time
from template.config import init

"""
Each slot in a bufferpool consists of an entire base page, and each page can hold up to 512 records. 
"""

class Slot:

    def __init__(self, key, page_id, file_name, pages):
        self.is_clean = True # Represents whether it has been updated or not
        self.table_key = key # Key that identifies the table that the pages came from
        self.page_id = page_id # Within the table, the pageID that the memory in the slot corresponds to
        self.pages = pages # The actual list of page objects representing the data
        self.file_name = file_name # So that the file can be written to once the slot needs to be evicted.


class Bufferpool:

    def __init__(self, tables):
        self.path = "" # Path represents the path to the folder where all files are. Empty initially.
        self.slots = [] # 16 slots for 16 base pages/tail pages of different tables in the database
        for i in range(0, len(self.slots)):
            slot = Slot()
            self.slots.append(slot)

    # Set the path
    def set_path(self, path):
        self.path = path

    # Evict a slot from the base page by popping off the last element.
    def evict_least_used(self):
        self.slots.pop(len(self.slots)-1)

    # To keep the least used at the back, we want to move every slot we're updating to the front
    def move_to_front(self, i):
        slot = self.slots[i]
        self.slots.pop(i)
        self.slots.insert(0, slot)

    # Returns the index of a specified base/tail page with page_id that belongs to a table with the key
    def index_of(self, table_key, page_id):
        for i in range(0, len(self.slots)):
            if self.slots[i].table_key == table_key and self.slots[i].page_id == page_id:
                return i
        return -1

    def write_to_file(self, i):
        slot = self.slots[i]
        pages = slot.pages
        path_string = self.path + "/" + slot.file_name
        with open(path_string, 'w') as page_file:
            for i in range(0, pages[0].num_records):
                offset = i
                row_string = ""
                for j in range(0, len(pages)):
                    row_string += str(pages[j].read(i))
                    if (j == len(pages)-1):
                        row_string += '\n'
                    else:
                        row_string += ' '
                page_file.write(row_string)


