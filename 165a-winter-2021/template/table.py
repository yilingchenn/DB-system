from template.page import *
from template.index import Index
import datetime

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    """
    :param rid         #Generated rid based on the table
    :param key         #Unique identifier for the record.
    :param num_columns #Number of Columns in the table for which the record is a
        part of
    """
    def __init__(self, rid, key, num_cols):
        self.rid = rid
        self.key = key
        self.columns = [None, rid, datetime.datetime.now(), num_cols*'0']

class Table:

    """
    :param name     # Name of table
    :param key      # Table key
    :param num_columns # number of columns in the table
    Page directory maps RID to the index of the record in all of the base pages
    Index map maps the record key to the RID
    Not sure what self.index is yet
    Pages is a list of pages, each page corresponds to the corresponding column
    in the table
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        # page_directory maps the RID to the index at which the record occurs
        #   in ALL of the base pages.
        self.page_directory = {} # RID --> (pageID, offset)
        # index_map maps the key of the record to the RID
        self.index_map = {}
        self.index = Index(self)
        # self.pages[0] is the record object which holds crucial information
        # like the key/RID/indirection, while the rest of the columns
        # correspond to the actual values of the record
        self.pages = [Page()] * num_columns
        self.current_rid = 0

    """
    Generates a unique RID within the TABLE (not overall database) for a new
    record by counting up every time a new one is generated.
    """
    def generate_RID(self):
        self.current_rid += 1
        return self.current_rid

    def __merge(self):
        pass
