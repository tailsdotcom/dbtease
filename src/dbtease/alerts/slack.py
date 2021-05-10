"""Base alerter class."""

import logging

from typing import List

from dbtease.alerts.base import Alerter


import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class SlackAlerter(Alerter):
    """Emit Alters to the python logger.

    This is mostly for testing.
    """

    def __init__(self, alert_on: List[str], channel=None):
        self.client = WebClient(token=os.environ["DBTEASE_SLACK_TOKEN"])
        self.channel = channel or os.environ["DBTEASE_SLACK_CHANNEL"]
        super().__init__(alert_on=alert_on)

    def alert(self, alert_event: str, success: bool, message: str, metadata=None):
        """Slack Logging."""
        success_fail = "SUCCESS" if success else "FAIL"
        try:
            self.client.chat_postMessage(
                channel=self.channel,
                text=f"[test dbtease message] {alert_event} {success_fail}: {message}",
            )
        except SlackApiError as err:
            logging.error(f"Slack Alert Error: {err.response['error']}")
            raise err
