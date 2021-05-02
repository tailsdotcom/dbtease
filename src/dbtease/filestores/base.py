"""Base filestore class."""

from abc import ABC, abstractmethod
from typing import Dict


class Filestore(ABC):
    """Base interactions with a filestore."""

    @abstractmethod
    def __init__(self, **kwargs):
        ...

    @classmethod
    def from_dict(cls, config: Dict):
        return cls(**config)

    def check_access(self):
        return True

    @abstractmethod
    def upload_files(self, *paths: str):
        ...
