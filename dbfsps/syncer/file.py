import os
import logging
from typing import List
from hashlib import sha256


class File:
    """
    :param relpath:
        Path to the file relative to the package
    :param relpackagepath:
        Path to the folder containing the package, relative to the root
    :param root_dir:
        Absolute path to the root
    :param hashstr:
        Option to overwrite the hash from another version of the file
    :param relpath_remote:
        Optional different remote relative path, needed for the requirements.txt file for example.
        By default, self.relpath_remote is simply self.path.
    """
    def __init__(
            self, relpath: str, relpackagepath: str, root_dir: str,
            hashstr: str = None, relpath_remote: str = None):
        self.logger = logging.getLogger(__name__)
        self.path = relpath
        self.package = relpackagepath
        self.root = root_dir
        self.path_abs = os.path.join(self.root, self.package, self.path)

        if not hashstr:
            self.hash = self._generate_hash()
        else:
            self.hash = hashstr

        if not relpath_remote:
            self.path_remote = self.path
        else:
            self.path_remote = relpath_remote

    def _generate_hash(self) -> str:
        hashstr = calculate_file_hash(self.path_abs)
        self.logger.debug(f"Generated new hash for {self.path} ({self.path_abs})")
        return hashstr

    def __eq__(self, other) -> bool:
        if self.hash == other.hash:
            return True
        else:
            return False


def sort_list_of_files(files: List[File]) -> List[File]:
    """
    Sorts a list of File instances based on their `path` attribute,
    considering the file-tree structure.

    :param files:

    :returns:
        Sorted list of File instances based on their `path` attribute.
    """
    def get_sort_key(obj):
        path = os.path.normpath(obj.path)
        return path.count(os.sep), path

    return sorted(files, key=get_sort_key)


def calculate_file_hash(path_abs: str) -> str:
    if not os.path.isfile(path_abs):
        raise FileNotFoundError(f"Unable to calculate hash. File {path_abs} does not exist")
    with open(path_abs, 'rb') as f:
        hashstr = sha256(f.read()).hexdigest()
    return hashstr
