"""Define the schema object."""

import os.path

from dbtease.cron import refresh_due


class DbtSchema:
    def __init__(
        self,
        name,
        paths,
        schedule=None,
        depends_on=None,
        materialized=False,
        build=None,
        schemas=None,
        triggers_full_deploy=False,
    ):
        self.name = name
        self.paths = paths
        self.schedule = schedule
        self.depends_on = depends_on
        self.materialized = materialized
        self.build_config = build or {}
        self.schemas = schemas or [name]
        self.triggers_full_deploy = triggers_full_deploy
        if self.materialized and not self.schedule:
            raise ValueError(f"Schema {self.name} is materialized but has no schedule!")

    def __repr__(self):
        return f"<DbtSchema: {self.name}>"

    def matches_paths(self, paths):
        self_paths = [os.path.realpath(path) for path in self.paths]
        matched_paths = set()
        for self_path in self_paths:
            matched_paths |= set(
                path for path in paths if os.path.realpath(path).startswith(self_path)
            )
        return matched_paths

    def selector(self):
        selectors = ["path:" + path for path in self.paths]
        return " ".join(selectors)

    def refresh_due(self, last_refresh):
        """Work out whether a refresh is due based on cron and last refresh."""
        if not self.schedule:
            return False
        return refresh_due(self.schedule, last_refresh)

    @classmethod
    def from_dict(cls, name, config):
        """Make a schema object from a config dict and name."""
        return cls(name=name, **config)
