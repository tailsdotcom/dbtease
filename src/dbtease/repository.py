"""Defines repositories which store state."""

import json


class DictStateRepository:
    """Basic state responsitory using an in memory dict."""

    def __init__(self):
        self._state = {}

    def get_current_deployed(self):
        """Get the details of the currently deployed state."""
        return self._state.get("current_deployed", None)

    def set_current_deployed(self, commit_hash):
        """Sets the details of the currently deployed state."""
        self._state["current_deployed"] = commit_hash


class JsonStateRepository(DictStateRepository):
    """Basic state responsitory using an in memory dict."""

    def __init__(self, fname="state.json"):
        self._fname = fname

    def _load_state(self):
        try:
            with open(self._fname) as state_file:
                return json.load(state_file)
        except FileNotFoundError:
            return {}

    def _save_state(self, state):
        with open(self._fname, "w") as state_file:
            return json.dump(state, state_file)

    def get_current_deployed(self):
        """Get the details of the currently deployed state."""
        state = self._load_state()
        return state.get("current_deployed", None)

    def set_current_deployed(self, commit_hash):
        """Sets the details of the currently deployed state."""
        state = self._load_state()
        state["current_deployed"] = commit_hash
        self._save_state(state)
