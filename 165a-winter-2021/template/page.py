from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0 # offsets
        self.tail_page = []
        self.data = bytearray(4096) # base page size

    def has_capacity(self):
        pass

    def write_base_page(self, value):
        self.num_records += 1
        self.data(value)
        # write = append to base page
        pass
    
    def write_tail_page(self, value):
        # write = append to tail page
        pass
