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


class DbtProfiles(YamlFileObject):

    default_file_name = "profiles.yml"

    def __init__(self, profiles_obj, profile):
        self.profiles_obj = profiles_obj
        self.profile = profile

    @classmethod
    def from_dict(cls, config, profile=None):
        """Load a project from a dict."""
        # Only keep the config and profile elements
        profiles_obj = {}
        # Bring across any config values if present
        if "config" in config:
            profiles_obj["config"] = config["config"]
        if not profile:
            raise ValueError("Must provide a profile!")
        # Bring across the target profile
        profiles_obj[profile] = config[profile]
        return cls(profiles_obj=profiles_obj, profile=profile)
    
    def get_target_dict(self, target=None):
        # Get the main profile
        profile_dict = self.profiles_obj[self.profile]
        # Use default target if not given
        target = target or profile_dict.get("target", None)
        target_dict = profile_dict["outputs"][target]
        assert target_dict["type"] == "snowflake"
        return target_dict
