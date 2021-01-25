from template.page import *
from template.index import Index
from time import time

INDIRECTION_COLUMN = 0 # gives the direction in the tail page
RID_COLUMN = 1 # columns that contains RIDs
TIMESTAMP_COLUMN = 2 # mark when the first time the thing is installed
SCHEMA_ENCODING_COLUMN = 3


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
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} #{RID: records}
        self.index = Index(self)
        self.page = [Page()] * num_columns
        pass

    def __merge(self):
        pass
