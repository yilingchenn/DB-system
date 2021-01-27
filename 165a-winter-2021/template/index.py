"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        pass

    """
    # returns the location of all records with the given value on column "column"
    """
    
        def locate(self, column, value):
        # wouldn't locate just be like
        # return RID for pageID = column and offset = value
        # reverse search in dictionary
        rids = []
        page_directory = self.table.page_directory
        for rid, pageID, offset in page_directory.items():  
            if pageID == column and offset == value:
                # ** someone check if this is correct
                rids += rid
            else:
                return False
        return rids

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        # might need nested list since its a range
        rids_col = []
        rids_range = []
        page_directory = self.table.page_directory
        for i in range(begin, end):
            for rid, pageID, offset in page_directory.items():  
                if pageID == column and offset == i:
                    rids_col += rid
                else:
                    return False
            rids_range += rids_col
        return rids_range

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        
        pass
