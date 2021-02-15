from template.table import Table, Record
from template.index import Index
from time import time
from template.config import init
import os


class Bufferpool:

    # Can't initialize without the path, so set all fields to empty
    def __init__(self, tables):
        self.path = ""
        self.slots = {}

    # Each slot in the bufferpool is one base page.
    def set_path(self, path):
        self.path = path

    # Naive way of thinking about insert.
    def write_disk(self):
        # Write each of the 16 slots into a separate file
        pass

    def insert_column(self, column, table_name):
        pass

    def is_dirty(self):
        pass

    def get_file(self, pageId, tablename):
        path = self.path
        # Specify the file name
        file = str(tablename) + '_' + str(pageId) + '.txt'
        # Creating a file at specified location
        with open(os.path.join(path, file), 'r') as ff:
            f = ff.read()
            self.slot = {file: f}
        # make sure the length is < 16
        # evict if over 16
        # write the dirty page
        # evict

    def write_file(self, pageId, tablename):
        # think about dirty bit
        path = self.path
        # Specify the file name
        file = str(tablename) + '_' + str(pageId) + '.txt'
        # Creating a file at specified location
        with open(os.path.join(path, file), 'w') as ff:
            ff.write(self.slots[file])
