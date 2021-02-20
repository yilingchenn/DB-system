from template.table import Table
from template.bufferpool import Bufferpool
import os

class Database():

    def __init__(self):
        self.tables = []
        self.bufferpool = Bufferpool()

    def open(self, path):
        # Load all tables from files into self.tables
        self.bufferpool.set_path(path)
        if not os.path.exists(path):
            os.makedirs(path)
        # Path = path to folder where all of our files will be
        # Convert all files to list. Return list.
        # Initialize the bufferpool to empty slots. You only open once.

    def close(self):
        # Put dirty pages back into disk, writing to disk.
        # Have something in bufferpool that manages all of that for simplicity
        self.bufferpool.write_all()
        # Save all table information into a file.
        for i in range(0, len(self.tables)):
            table = self.tables[i]
            table.save_table()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    # Append entire table or just the table name?
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.bufferpool)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    # TODO: figure out how to delete entities that represent this table in the bufferpool and in files
    def drop_table(self, name):
        index = -1
        for i in range(0, len(self.tables)):
            if self.tables[i].name == name:
                index = i
        # If the specified table was found, pop it
        if index > 0:
            self.tables.pop(index)

    """
    # Returns table with the passed name
    """
    # TODO: Milestone 2
    def get_table(self, name):
        for i in range(0, len(self.tables)):
            if self.tables[i].name == name:
                return self.tables[i]
        # Return false if no table with that name exists
        return False
