"""Test the common yaml loader."""

import pytest

from dbtease.common import YamlFileObject


class DummyFileObject(YamlFileObject):
    """A test object."""
    def __init__(self, obj):
        self.obj = obj

    @classmethod
    def from_dict(cls, config, **kwargs):
        """Make an object holding a dict."""
        return cls(obj=config)


def test__simple_yaml_load():
    """Test loading a simple yaml string."""
    test_string = "foo: bar"
    test_obj = DummyFileObject.from_string(test_string)
    assert isinstance(test_obj, DummyFileObject)
    assert test_obj.obj == {"foo": "bar"}


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("foo", "foo"),
        ("some{# comment #}thing", "something"),
        ("{% for elem in ['foo', 'bar']%}{{elem}}{% endfor %}", "foobar"),
        # This assumes that SOME_ENV_VALUE doesn't actually exist.
        ("{{ env_var('SOME_ENV_VALUE', default='baz') }}", "baz"),
    ]
)
def test__common_jinja_template(test_input, expected):
    """Test jinja rendering."""
    assert YamlFileObject._template_string(test_input) == expected
