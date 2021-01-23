from template.page import *
from template.index import Index
from template.index import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    """
    Delete the record by setting the RID to a specified number (-1)
    """
    def delete(self):
        self.rid = -1
        self.columns[RID_COLUMN] = self.rid


    """
    Update the record by setting the indirection to the new specified index in
    the corresponding tail_page
    """
    def update(self, new_indirection_column):
        self.columns[INDIRECTION_COLUMN] = new_indirection_column



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
        self.page_directory = {}
        self.index = Index(self)
        self.pages = [Page()] * num_columns
        pass

    def __merge(self):
        pass
