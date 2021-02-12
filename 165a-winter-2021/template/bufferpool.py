from template.table import Table, Record
from template.index import Index
from time import time
from template.config import init

class Bufferpool:

    # Can't initialize without the path, so set all fields to empty
    def __init__(self, tables):
        self.path = ""
        self.slots = [None] * 16

    # Each slot in the bufferpool is one base page.
    def set_path(self, path):
        self.path = path

    # Naive way of thinking about insert.
    def write_disk(self):
        # Write each of the 16 slots into a separate file

    def insert_column(self, column, table_name):


"""
What are the steps to Insertion?
Insert something into a table --> It's completely new! 1st thing ever.
-Do typical insert stuff: rid, indirection, columns, timestamp
-Create an empty file, add some sort of logic that makes sure that the base page
corresponds to that file
"""

"""

"""


