from template.page import *
from template.index import Index
from template.page import Page
from time import time

# Indirection column gives the location of the updated version of the data in the tail pages,
#   if it exists. Set to None if not and an index in another
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
# When schema encoding is 1, it has been updated and the new value is in the tail pages
# When schema encoding is 0, it has not been updated and value is in the base pages
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid # RID = location in memory
        self.key = key # For the user: how they specify which row they want to change
        # What about the indirection column??
        self.columns = columns
        # self.columns = [2, 7, 9, 4]
        # indirection = 2
        # RID = 7
        # timestamp = 9
        # schema_encoding = 4

    """
    Delete the record by setting the RID to a specified number (-1)
    """
    def delete(self):
        self.rid = -1
        self.columns[RID_COLUMN] = -1


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
