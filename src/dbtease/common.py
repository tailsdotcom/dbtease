"""Common framework for loading yaml files."""

import yaml
import os.path

from jinja2.sandbox import SandboxedEnvironment


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
    def _template_string(cls, raw_string: str) -> str:
        """Render a jinja templated string.

        Reference for macros: https://github.com/fishtown-analytics/dbt/blob/cee0bfbfa2596520032b766fd1027fe748777c75/core/dbt/context/base.py#L275
        """
        jinja_context: dict = {}

        jinja_env = SandboxedEnvironment(
            # The do extension allows the "do" directive
            autoescape=False,
            extensions=["jinja2.ext.do"],
        )

        template = jinja_env.from_string(raw_string, globals=jinja_context)

        return template.render()

    @classmethod
    def from_string(cls, raw_string, **kwargs):
        """Load a object from a string.

        This applies jinja templating.
        """
        rendered_string = cls._template_string(raw_string)
        config_dict = yaml.safe_load(rendered_string)
        return cls.from_dict(config_dict, **kwargs)

    @classmethod
    def from_file(cls, fname, **kwargs):
        """Load a object from a file."""
        with open(fname) as raw_file:
            raw_string = raw_file.read()
        return cls.from_string(raw_string, **kwargs)

    @classmethod
    def from_path(cls, path, fname=None, **kwargs):
        """Load a file from a path."""
        # Expand user if relevant.
        path = os.path.expanduser(path)
        return cls.from_file(
            fname=os.path.join(path, fname or cls.default_file_name), **kwargs
        )
