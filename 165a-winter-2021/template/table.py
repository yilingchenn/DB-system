from page import *
from index import Index
from time import time

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns # external columns

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
        # Need to iterate through and allocate a new page every time to avoid changing all pages
        self.page = []
        for i in range(0, self.total_columns):
            new_page = Page()
            self.page.append(new_page)
        self.rid_counter = 0
        self.page_range = 1

    # generate RID
    def gen_rid(self):
        self.rid_counter += 1
        return self.rid_counter

    # If one of the pages does not have capacity, then all pages won't have capacity,
    # and another page range needs to be added to account for more pages.
    def checker(self):
        # Check the capacity of the current page range
        if not self.page[(self.page_range - 1) * self.total_columns].has_capacity():
            # Allocate new pages
            for i in range(0, self.total_columns):
                new_page = Page()
                self.page.append(new_page)
            # Add one to the page_range
            self.page_range += 1


    # Don't have to implement for this cycle
    def __merge(self):
        pass
