# Global Setting for the Database
# PageSize, StartRID, etc..

class init:

    def __init__(self):
        self.page_size = 4096 # 4K
        self.page_range_size = 5
        self.max_int = 18446744073709551615
        self.bufferpool_size = 16
