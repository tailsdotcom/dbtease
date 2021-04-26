"""Methods for interacting with dbt."""

from dbtease.common import YamlFileObject


class DbtProject(YamlFileObject):

    default_file_name = "dbt_project.yml"

    def __init__(self, package_name, profile_name):
        self.package_name = package_name
        self.profile_name = profile_name

    @classmethod
    def from_dict(cls, config):
        """Load a project from a dict."""
        return cls(
            package_name=config["name"],
            profile_name=config["profile"]
        )
