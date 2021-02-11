from template.config import *

class Page:

    def __init__(self):
        self.num_records = 0 # length of base pages
        self.config = init() # call the config class
        self.data = bytearray(self.config.page_size) # page size
        # lineage refers to the number of updates that have been merged successfully. So if the lineage of a page is 17
        # and a record in the tail page has an offset of 18, then we need to read from the tail page. However, if the
        # offset of the record in the tail page is 11, then we're good.
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

    def read(self, offset):
        return self.data[offset*8: (offset+1)*8]

