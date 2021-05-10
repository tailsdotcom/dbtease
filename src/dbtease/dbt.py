"""Methods for interacting with dbt."""

import yaml
import copy
import os.path

from dbtease.common import YamlFileObject


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

    def generate_patched_yml(self, database=None, target=None):
        new_profiles_obj = copy.deepcopy(self.profiles_obj)
        # Get detault target if not set
        target = target or new_profiles_obj[self.profile]["target"]
        # Remove any other targets
        for profile_target in list(new_profiles_obj[self.profile]["outputs"].keys()):
            if profile_target != target:
                new_profiles_obj[self.profile]["outputs"].pop(profile_target)
        # Patch database (if provided)
        if database:
            new_profiles_obj[self.profile]["outputs"][target]["database"] = database
        return yaml.dump(new_profiles_obj)

    def get_default_database(self, target=None):
        # Get detault target if not set
        target = target or self.profiles_obj[self.profile]["target"]
        return self.profiles_obj[self.profile]["outputs"][target]["database"]


class DbtProject(YamlFileObject):

    default_file_name = "dbt_project.yml"

    def __init__(self, package_name, profile_name):
        self.package_name = package_name
        self.profile_name = profile_name

    @classmethod
    def from_dict(cls, config):
        """Load a project from a dict."""
        return cls(package_name=config["name"], profile_name=config["profile"])

    def generate_profiles_yml(self, profiles_dir="~/.dbt/", database=None, target=None):
        profiles_dir = os.path.expanduser(profiles_dir)
        parent_profiles = DbtProfiles.from_path(
            path=profiles_dir, profile=self.profile_name
        )
        return parent_profiles.generate_patched_yml(database=database, target=target)

    def get_default_database(self, profiles_dir="~/.dbt/", target=None):
        profiles_dir = os.path.expanduser(profiles_dir)
        parent_profiles = DbtProfiles.from_path(
            path=profiles_dir, profile=self.profile_name
        )
        return parent_profiles.get_default_database(target=target)
