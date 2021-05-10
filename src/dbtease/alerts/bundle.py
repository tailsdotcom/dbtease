"""Alerter Bundle"""

import logging
from typing import List

from dbtease.alerts.base import Alerter
from dbtease.alerts.logger import LoggingAlerter
from dbtease.alerts.slack import SlackAlerter


class AlterterBundle:
    def __init__(self, alerters: List[Alerter]):
        self.alerters = alerters

    def handle_event(
        self, alert_event: str, success: bool, message: str, metadata=None
    ):
        """Broadcast this event to all the handlers."""
        for alerter in self.alerters:
            # Failures in alerting should stop the process however.
            try:
                alerter.handle_event(
                    alert_event=alert_event,
                    success=success,
                    message=message,
                    metadata=metadata,
                )
            except Exception as err:
                logging.error(f"Error handling event: {err}")

    @classmethod
    def from_config(cls, config):
        lookup = {
            "logger": LoggingAlerter,
            "slack": SlackAlerter,
        }
        alerters = []
        for alert_config in config:
            alerter_type = alert_config.pop("method")
            alerter_class = lookup[alerter_type]
            alerters.append(alerter_class(**alert_config))
        return cls(alerters=alerters)
