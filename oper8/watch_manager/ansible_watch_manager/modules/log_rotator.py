"""
Helper module to implement a once-per-open log rotating handler
"""

# Standard
from logging.handlers import RotatingFileHandler
import os


class AutoRotatingFileHandler(RotatingFileHandler):
    """This subclass of the RotatingFileHandler will perform a rotation once on
    construction. The result is that the previous log file will be backed up
    before opening the new file, but the file will never rotate during a single
    reconcile loop.
    """

    def __init__(self, filename, backupCount=10):
        """Construct with only the filename and backupCount args of the base
        class.

        Args:
            filename:  str
                The name of the primary log file to manage
            backupCount:  int
                The number of backed up copies of the log file to keep
        """
        file_already_there = os.path.exists(filename)
        super().__init__(filename, backupCount=backupCount, maxBytes=0)
        if file_already_there:
            self.doRollover()
