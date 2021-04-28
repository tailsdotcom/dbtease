"""Base warehouse class."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Union, Tuple, Dict, Optional


@dataclass
class Sql:
    """Container for a statment with optional parameters."""
    sql: str
    params: Optional[Union[Tuple, Dict]] = None

    def __iter__(self):
        """Iterator to call *Sql on an execute."""
        if self.params:
            return iter((self.sql, self.params))
        else:
            return iter((self.sql,))

    def __str__(self):
        return repr(self.sql)


class Warehouse(ABC):
    """Base interactions with warehouse."""

    @classmethod
    def from_target(cls, target_dict: Dict):
        return cls(**target_dict)

    @abstractmethod
    def get_current_deployed(self, project_name: str) -> Optional[str]:
        ...

    @abstractmethod
    def deploy(self, project_name: str, commit_hash: str, manifest: str, build_db: str, deploy_db: str):
        ...
