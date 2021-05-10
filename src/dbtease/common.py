"""Common framework for loading yaml files."""

import yaml
import os.path


class YamlFileObject:
    """Common class for a yaml file object."""

    default_file_name = "default.yaml"

    @classmethod
    def from_dict(cls, config, **kwargs):
        """Load a schedule from a dict."""
        return NotImplementedError(
            "from_dict method not overriden for {0}".format(cls.__name__)
        )

    @classmethod
    def from_file(cls, fname, **kwargs):
        """Load a object from a file."""
        with open(fname) as yaml_file:
            config_dict = yaml.safe_load(yaml_file.read())
        return cls.from_dict(config_dict, **kwargs)

    @classmethod
    def from_path(cls, path, fname=None, **kwargs):
        """Load a file from a path."""
        return cls.from_file(
            fname=os.path.join(path, fname or cls.default_file_name), **kwargs
        )
