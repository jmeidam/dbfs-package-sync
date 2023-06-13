import os
import logging
from dbfsps.syncer.file import File


class State:
    """State of the remote files.
    Contains a list of files and their hashes that should currently be on DBFS

    :param root_dir:
        Absolute path to the root dir of the repository where you can find pyproject.toml
    :param relpackagepath:
        Relative path to the package directory (usually the package name)
    :param statefilename:
        Name of the statefile to use. Is .dbfsps_file_status by default and is located in the root
    """
    def __init__(self, root_dir: str, relpackagepath: str, statefilename: str = '.dbfsps_file_status'):
        self.logger = logging.getLogger(__name__)
        self.files = {}
        self.root = root_dir
        self.package = relpackagepath
        self.statefilepath = os.path.join(self.root, statefilename)
        self.packagepath = os.path.join(self.root, self.package)

        if os.path.isfile(self.statefilepath):
            self.load_state()
        else:
            self.logger.debug("No statefile created yet")

    def load_state(self):
        """Load files and their hashes from the statefile"""
        with open(self.statefilepath, 'r') as f:
            self.logger.debug(f"Loading statefile at {self.statefilepath}")
            for line in f.readlines():
                vals = line.strip().split(",")
                relpath = vals[0]
                hashstr = vals[1]
                file = File(relpath, self.package, self.root, hashstr=hashstr)
                self.files[file.path] = file

    def store_state(self):
        """Store the current files and their hashes in the statefile"""
        with open(self.statefilepath, 'w') as f:
            for file in self.files.values():
                f.write(f"{file.path},{file.hash}\n")
