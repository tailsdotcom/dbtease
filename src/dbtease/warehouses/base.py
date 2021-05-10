"""Base warehouse class."""

import datetime

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Union, Tuple, Dict, Optional, List

import click
import uuid
from contextlib import contextmanager


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

    FULL_DEPLOY = "<full-deploy>"

    @abstractmethod
    def __init__(self, **kwargs):
        ...

    @classmethod
    def from_target(cls, target_dict: Dict):
        return cls(**target_dict)

    @abstractmethod
    def get_current_deployed(self, project_name: str) -> Optional[str]:
        ...

    @abstractmethod
    def deploy(
        self,
        project_name: str,
        commit_hash: str,
        schemas: List[str],
        build_db: str,
        deploy_db: str,
        build_timestamp: datetime.datetime,
    ) -> None:
        ...

    @abstractmethod
    def acquire_lock(self, target: str, ttl_minutes=1) -> Optional[str]:
        ...

    @abstractmethod
    def release_lock(self, target: str, lock_key: str) -> None:
        ...

    @contextmanager
    def lock(self, target: str, ttl_minutes: int = 1):
        """Context Manager which implements acquire and release lock."""
        lock_key = self.acquire_lock(target=target, ttl_minutes=ttl_minutes)
        if not lock_key:
            raise click.ClickException(
                f"Unable to lock {target!r}. Someone else has the lock. Try again later."
            )
        try:
            yield
        finally:
            self.release_lock(target, lock_key)


class DummyWarehouse(Warehouse):
    """Dummy warehouse for testing."""

    def __init__(self, live_hash=None, **kwargs):
        self.live_hash = live_hash
        self._locks = {}

    def get_current_deployed(self, project_name: str) -> Optional[str]:
        return self.live_hash

    def deploy(
        self,
        project_name: str,
        commit_hash: str,
        schemas: List[str],
        build_db: str,
        deploy_db: str,
        build_timestamp: datetime.datetime,
    ) -> None:
        self.live_hash = commit_hash

    def acquire_lock(self, target: str, ttl_minutes=1):
        key = uuid.uuid4()
        self._locks[target] = key
        return key

    def release_lock(self, target: str, lock_key: str):
        if self._locks[target] == lock_key:
            del self._locks[target]
