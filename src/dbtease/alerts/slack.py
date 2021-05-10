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
        metadata = metadata or {}
        schema = metadata.get("schema_name", None)

        slack_emoji = ":sparkle:" if success else ":exclamation:"

        message_text = f"{message}"
        if schema:
            message_text += f" Schema: *{schema}*"

        slack_block = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{slack_emoji} *{success_fail}*: {message_text}.",
            },
        }

        try:
            self.client.chat_postMessage(
                channel=self.channel,
                # Fallback message text
                text=message_text,
                # Blocks will be the main display
                blocks=[slack_block],
            )
        except SlackApiError as err:
            logging.error(f"Slack Alert Error: {err.response['error']}")
            raise err
