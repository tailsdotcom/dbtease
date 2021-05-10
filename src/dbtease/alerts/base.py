"""Base alerter class."""

from typing import List
from abc import ABC, abstractmethod


class Alerter(ABC):
    """Base interactions with an alterting backend."""

    def __init__(self, alert_on: List[str]):
        self.alert_on = alert_on

    def _handles_event(self, alert_event):
        """Should we handle this event."""
        return alert_event in self.alert_on

    def handle_event(
        self, alert_event: str, success: bool, message: str, metadata=None
    ):
        """Alert if we handle this kind of event."""
        if self._handles_event(alert_event):
            self.alert(
                alert_event=alert_event,
                success=success,
                message=message,
                metadata=metadata,
            )

    @abstractmethod
    def alert(self, alert_event: str, success: bool, message: str, metadata=None):
        ...
