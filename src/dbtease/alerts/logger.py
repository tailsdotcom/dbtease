"""Base alerter class."""

import logging

from typing import List

from dbtease.alerts.base import Alerter


class LoggingAlerter(Alerter):
    """Emit Alters to the python logger.

    This is mostly for testing.
    """

    def __init__(self, alert_on: List[str], logger="dbtease.alerts", level="INFO"):
        self.logger = logging.getLogger(logger)
        self.level = {
            "CRITICAL": 50,
            "ERROR": 40,
            "WARNING": 30,
            "INFO": 20,
            "DEBUG": 10,
        }[level]
        super().__init__(alert_on=alert_on)

    def alert(self, alert_event: str, success: bool, message: str, metadata=None):
        """Basic Logging."""
        success_fail = "SUCCESS" if success else "FAIL"
        self.logger.log(self.level, f"{alert_event} {success_fail}: {message}")
