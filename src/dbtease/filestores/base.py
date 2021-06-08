"""Base filestore class."""

import logging
from abc import ABC, abstractmethod
from typing import Dict


class Filestore(ABC):
    """Base interactions with a filestore."""

    @abstractmethod
    def __init__(self, **kwargs):
        ...

    @classmethod
    def from_dict(cls, config: Dict, **kwargs):
        return cls(**config, **kwargs)

    @abstractmethod
    def _upload_filestr(self, fname, content):
        ...

    def check_access(self):
        """Test that we can write to the dest folder."""
        try:
            self._upload_filestr(".testfile", "test")
        except Exception as err:
            logging.warning(err)
            return False
        return True

    @abstractmethod
    def upload_files(self, *paths: str):
        ...
