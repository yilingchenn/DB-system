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

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        # Total columns = num_columns + 4 internal columns (RID, Indirection, Schema, Timestamp)
        self.total_columns = num_columns + 4
        self.num_columns = num_columns
        self.page_directory = {} #{RID: (page_range, offset)}
        self.index_directory = {} # {Key: RID}
        self.index = Index(self) # Not sure what to do with this right now
        self.page = [Page()] * total_columns
        self.rid_counter = 0
        self.page_range = 1

    def gen_rid(self):
        # generate RID
        self.counter += 1
        return self.counter

    # If one of the pages does not have capacity, then all pages won't have capacity,
    # and another page range needs to be added to account for more pages.
    def checker(self):
        if not self.page[0].has_capacity():
            self.page = self.page + [Page()] * self.total_columns
            page_range += 1

    def __merge(self):
        pass
