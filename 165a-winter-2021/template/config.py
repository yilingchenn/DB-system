# Global Setting for the Database
# PageSize, StartRID, etc..

class Config:

    def __init__(self):
        self.page_size = 4096 # 4K
        self.page_range_size = 2