from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.base_page = []
        self.tail_page = []

    def has_capacity(self):
        pass

    def write(self, value):
        # self.num_records += 1
        # self.base_page.append(value)
        # new_record = Record(rid = new generated rid, key = ?, columns = ?, record_value = value, indirection_column = null)
        pass
