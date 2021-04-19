"""Define the schema object."""


class DbtSchema:
    def __init__(
        self, name, selectors, schedule=None,
        depends_on=None, materialized=False
    ):
        self.name = name
        self.selectors = selectors
        self.schedule = schedule
        self.depends_on = depends_on
        self.materialized = materialized

    def __repr__(self):
        return f"<DbtSchema: {self.name}>"

    @classmethod
    def from_dict(cls, name, config):
        """Make a schema object from a config dict and name."""
        return cls(name=name, **config)
