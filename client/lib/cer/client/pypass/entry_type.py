# Taken from https://github.com/aviau/python-pass

from enum import Enum


class EntryType(Enum):
    password = 1
    """password entry"""

    username = 2
    """username/login entry"""

    hostname = 3
    """hostname entry"""
