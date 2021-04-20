"""Init py for dbtease."""

# Set the version attribute of the library
import pkg_resources
import configparser

# Get the current version
config = configparser.ConfigParser()
config.read([pkg_resources.resource_filename("dbtease", "config.ini")])

__version__ = config.get("dbtease", "version")
