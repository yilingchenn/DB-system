from template.table import Table
from template.bufferpool import Bufferpool

class Database():

    def __init__(self):
        self.tables = []
        # Bufferpool is a list of of page objects --> The only thing not on disk
        self.bufferpool = []
        pass

    def open(self, path):
        # What is path???
        # Open files for bufferpool, reading from the disk
        # Need algorithm to decide which page to discard when we need to read and write new data
        pass

    def close(self):
        # Put dirty pages back into disk, writing to disk
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    # TODO: MILESTONE 2
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        return table

    """
    # Deletes the specified table
    """
    # TODO: Milestone 2
    def drop_table(self, name):
        pass

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        pass
