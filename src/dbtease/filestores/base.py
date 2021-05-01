"""Base filestore class."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Union, Tuple, Dict, Optional


class Filestore(ABC):
    """Base interactions with a filestore."""

    @classmethod
    def from_dict(cls, config: Dict):
        return cls(**config)
    
    def check_access(self):
        return True
    
    @abstractmethod
    def upload_files(self, *paths: str):
        ...


