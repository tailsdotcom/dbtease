"""Defines repositories which store state."""

class DictStateRepository:
    """Basic state responsitory using an in memory dict."""

    def __init__(self):
        self._state = {}

    def get_current_deployed(self):
        """Get the details of the currently deployed state."""
        if "current_deployed" in self._state:
            return False
