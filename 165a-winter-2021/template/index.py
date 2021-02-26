"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        self.table = table

    # Returns if the key exists in the dictionary
    def key_exists(self, key):
        if key in self.table.index_directory.keys():
            # if self.select(key, 0, [1]*self.table.num_columns)[0].columns == [MAX_INT]*self.table.num_columns:
            rid = self.table.index_directory[key]
            temp = self.table.get_most_updated(rid)
            if temp == [self.table.config.max_int] * self.table.num_columns:
                return False
            else:
                return True
        else:
            return False

    """
    # returns the location of all records with the given value on column "column"
    """
    def locate(self, column, value):
        if self.indices[column] == None:
            self.create_index(column)
        # it will be the most updated since we update index in there
        dict = self.indices[column]
        rid = dict[value]
        # Rid is a list of rid's where value occurs
        return rid

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):
        rid_list = []
        for i in range(begin, end):
            rid_list.append(self.locate(column, i))
        # Return list might be a list of lists!
        return rid_list

    """
    # Create index on specific column
    """
    def create_index(self, column_number):
        new_index = {}
        # Loop through all the keys
        keys = list(self.table.index_directory.keys())
        for i in range(0, len(keys)):
            if self.key_exists(keys[i]):
                rid = self.table.index_directory[keys[i]]
                most_updated = self.table.get_most_updated(rid)
                value = most_updated[column_number]
                if value in new_index:
                    new_index[value].append(rid)
                else:
                    new_index[value] = [rid]
        # At the end, put it in the list if indices
        self.indices[column_number] = new_index

    """
    # Drop index of specific column
    """
    def drop_index(self, column_number):
        self.indices[column_number] = None

    def update_index(self, rid, new, old, column):
        dict = self.indices[column]
        rid_list = dict[old]
        update_index = rid_list.index(rid)
        dict[old].pop(update_index)
        if new != None:
            dict[new].append(rid)