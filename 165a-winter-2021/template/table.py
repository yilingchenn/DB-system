from template.page import *
from template.index import Index
from time import time

class Record:

    def __init__(self, rid, key, indirection, timestamp, schema_encoding):
        self.rid = rid
        self.key = key
        self.indirection = indirection
        self.timestamp = timestamp
        self.schema_encoding = schema_encoding
        [rid, indirection, timestamp, schema_encoding]

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns + 4 # columns + RID + Indirection + Schema + Timestamp
        self.page_directory = {} #{RID: (page id, indices)}
        self.index = Index(self)
        self.page = [Page()] * num_columns
        self.counter = 0
        self.page_range = 1

    def gen_rid(self):
        # generate RID
        self.counter += 1
        return self.counter

    def checker(self):
        if self.page[len(self.page)-1].num_records == len(self.page.data)/8:
            self.page = self.page + [Page()] * num_columns
            page_range += 1

    def __merge(self):
        pass
