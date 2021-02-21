from template.table import Table
from template.bufferpool import Bufferpool
import os

class Database():

    def __init__(self):
        self.tables = []
        self.bufferpool = Bufferpool()

    def open(self, path):
        self.bufferpool.set_path(path)
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)

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
        table.create_new_file(1, name, 4)
        table.create_new_file(2, name, num_columns)
        return table

    """
    # Deletes the specified table
    """
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
    def get_table(self, name):
        for i in range(0, len(self.tables)):
            if self.tables[i].name == name:
                return self.tables[i]
        file_name_with_path = self.path + "/" + name + "_directory.txt"
        file_name_without_path = name + "_directory.txt"
        if os.path.isfile(file_name_with_path):
            # Read information in from the file and reconstruct table and return it
            return self.reconstruct_table(file_name_without_path, name)
        else:
            # No table with that name in files or in self.tables
            return False

    def construct_dictionary(self, keys, values):
        new_dictionary = {}
        for i in range(0, len(keys)):
            new_dictionary[keys[i]] = values[i]
        return new_dictionary

    def reconstruct_table(self, file_name, name):
        path = self.bufferpool.path
        with open(os.path.join(path, file_name), 'rb') as ff:
            f = ff.read()
            # Read RID counter
            rid_counter = int.from_bytes(f[0:8], byteorder='big')
            offset = 1
            # Read page directory keys
            page_directory_keys = []
            for i in range(0, rid_counter):
                start = offset*8
                end = (offset+1)*8
                page_directory_key = int.from_bytes(f[start:end], byteorder='big')
                page_directory_keys.append(page_directory_key)
                offset += 1
            # Read page directory values
            page_directory_values = []
            for i in range(0, rid_counter):
                page_directory_value_list = []
                for j in range(0, 3):
                    start = offset * 8
                    end = (offset + 1) * 8
                    page_directory_value = int.from_bytes(f[start:end], byteorder='big')
                    page_directory_value_list.append(page_directory_value)
                    offset += 1
                page_directory_values.append(page_directory_value_list)
            page_directory = self.construct_dictionary(page_directory_keys, page_directory_values)
            # Read length of index directory
            index_directory_length = int.from_bytes(f[offset*8:(offset+1)*8], byteorder='big')
            offset += 1
            index_directory_keys = []
            for i in range(0, index_directory_length):
                start = offset*8
                end = (offset+1)*8
                index_directory_key = int.from_bytes(f[start:end], byteorder='big')
                index_directory_keys.append(index_directory_key)
                offset += 1
            index_directory_values = []
            for i in range(0, index_directory_length):
                start = offset * 8
                end = (offset + 1) * 8
                index_directory_value = int.from_bytes(f[start:end], byteorder='big')
                index_directory_values.append(index_directory_value)
                offset += 1
            index_directory = self.construct_dictionary(index_directory_keys, index_directory_values)
            # Read number of columns
            num_columns = int.from_bytes(f[offset * 8:(offset + 1) * 8], byteorder='big')
            offset += 1
            # Read key
            key = int.from_bytes(f[offset * 8:(offset + 1) * 8], byteorder='big')
            offset += 1
            # Read length of base page internal and base page external
            base_page_length = int.from_bytes(f[offset * 8:(offset + 1) * 8], byteorder='big')
            offset += 1
            # Read base_page_internal
            base_page_internal = []
            for i in range(0, base_page_length):
                start = offset * 8
                end = (offset + 1) * 8
                base_page_internal_value = int.from_bytes(f[start:end], byteorder='big')
                base_page_internal.append(base_page_internal_value)
                offset += 1
            # Read base external
            base_page_external = []
            for i in range(0, base_page_length):
                start = offset * 8
                end = (offset + 1) * 8
                base_page_external_value = int.from_bytes(f[start:end], byteorder='big')
                base_page_external.append(base_page_external_value)
                offset += 1
            # Read length of tail page
            tail_page_length = int.from_bytes(f[offset * 8:(offset + 1) * 8], byteorder='big')
            offset += 1
            tail_page = []
            for i in range(0, tail_page_length):
                start = offset * 8
                end = (offset + 1) * 8
                tail_page_value = int.from_bytes(f[start:end], byteorder='big')
                tail_page.append(tail_page_value)
                offset += 1
            # Read number of pages
            num_pages = int.from_bytes(f[offset * 8:(offset + 1) * 8], byteorder='big')
        # Now, manually reconstruct the table object and return it
        new_table = Table(name, num_columns, key, self.bufferpool)
        new_table.page_directory = page_directory
        new_table.index_directory = index_directory
        new_table.rid_counter = rid_counter
        new_table.num_page = num_pages
        new_table.base_pages_internal = base_page_internal
        new_table.base_pages_external = base_page_external
        new_table.tail_pages = tail_page
        return new_table

