from config import *


class Page:

    def __init__(self):
        self.num_records = 0 # length of base pages
        self.num_updates = 0 # length of tail page
        self.tail_page = bytearray() # No specified size for tail page
        self.data = bytearray(4096) # base page size

    # Check that the page still has less than 512 entries
    def has_capacity(self):
        if self.num_records == len(self.data)/8:
            return False
        else:
            return True

    # Add a column to the base page. Because the key can be greater than 255,
    # the record will be at self.data[numrecords*8: numrecords+1*8]
    def write_base_page(self, value):
        # Check if the the value is a string or integer, which changes the way
        # you will encode in memory
        if isinstance(value, str):
            self.data[self.num_records*8: (self.num_records+1)*8] = value.encode()
        else:
            self.data[self.num_records*8: (self.num_records+1)*8] = value.to_bytes(8, byteorder = 'big')
        self.num_records += 1

    # Add a column to the tail page. Because the key can be greater than 255,
    # the record will be at self.data[numrecords*8: numrecords+1*8]
    def write_tail_page(self, value):
        # Check if the the value is a string or integer, which changes the way
        # you will encode in memory
        if isinstance(value, str):
            self.tail_page[self.num_updates*8: (self.num_updates+1)*8] = value.encode()
        else:
            self.tail_page[self.num_updates*8: (self.num_updates+1)*8] = value.to_bytes(8, byteorder = 'big')
        self.num_records += 1

    def read_base_page(self, offset):
        return self.data[offset*8: (offset+1)*8]

    def read_tail_page(self, offset):
        return self.tail_page[offset*8: (offset+1)*8]
