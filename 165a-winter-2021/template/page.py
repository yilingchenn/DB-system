from template.config import *


class Page:

    """
    Each page has a base page and tail page, both of which are represented as
    arrays.
    """
    def __init__(self):
        self.base_page = []
        self.tail_page = []

    """
    From the skeleton code. TODO: implement later, once memory is more of a
    pressing issue.
    """
    def has_capacity(self):
        pass

    """
    Returns the length of the base page.
    """
    def return_base_length(self):
        return len(self.base_page)

    """
    Returns the length of the tail page.
    """
    def return_tail_length(self):
        return len(self.tail_page)

    """
    Writes to the base page by appending the specified value to the end.
    """
    def write_base_page(self, value):
        self.base_page.append(value)

    """
    Writes to the tail page by appending the specified value to the end. 
    """
    def write_tail_page(self, value):
        self.tail_page.append(value)
