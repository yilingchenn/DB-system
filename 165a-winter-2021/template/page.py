from template.config import *
import threading

class Page:

    def __init__(self):
        self.num_records = 0 # length of base pages
        self.num_records_lock = threading.Lock()
        self.config = init() # call the config class
        self.data = bytearray(self.config.page_size) # page size
        # lineage refers to largest RID of a record in the tail page that has been merged into base pages successfully
        # Eg: If lineage is 1001 and we encounter a RID in tail page with RID 1000 we know that the most updated is
        # If lineage 1001 and we encounter an indirection in base page that is 1002 we know we must go to the tail page
        self.lineage = 0

        # Check that the page still has less than 512 entries
    def has_capacity(self):
        if self.num_records == len(self.data)/8:
            return False
        else:
            return True

    # Change the value of the base page at that offset to a new value. Used only for schema encoding and indirection.
    def edit(self, offset, new_val):
        self.data[offset*8: (offset+1)*8] = new_val.to_bytes(8, byteorder = 'big')

    def write(self, value):
        self.data[self.num_records * 8: (self.num_records + 1) * 8] = value.to_bytes(8, byteorder='big')
        self.num_records += 1
        print("incremented num_records to ", self.num_records)

    def read(self, offset):
        return self.data[offset*8: (offset+1)*8]

    def get_num_records(self):
        return self.num_records

