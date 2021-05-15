#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""The script for setting up dbtease."""

from __future__ import absolute_import
from __future__ import print_function

import io
import sys

if sys.version_info[0] < 3:
    raise Exception(
        "dbtease does not support Python 2. Please upgrade to Python 3."
    )

import configparser
from os.path import dirname
from os.path import join

from setuptools import find_packages, setup


# Get the global config info as currently stated
# (we use the config file to avoid actually loading any python here)
config = configparser.ConfigParser()
config.read(["src/dbtease/config.ini"])
version = config.get("dbtease", "version")


def read(*names, **kwargs):
    """Read a file and return the contents as a string."""
    return io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8"),
    ).read()


setup(
    name="dbtease",
    version=version,
    license="MIT License",
    description="CLI for deploying large dbt projects in pieces.",
    long_description=read("README.md") + "\n\n---\n\n" + read("CHANGELOG.md"),
    # Make sure pypi is expecting markdown!
    long_description_content_type="text/markdown",
    author="Alan Cruickshank",
    author_email="alan@tails.com",
    url="https://github.com/tailsdotcom/dbtease",
    python_requires=">=3.6",
    keywords=["dbt"],
    project_urls={
        # "Homepage": "https://github.com/tailsdotcom/dbtease",
        # "Documentation": "https://github.com/tailsdotcom/dbtease",
        "Source": "https://github.com/tailsdotcom/dbtease",
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    classifiers=[
        # complete classifier list:
        # http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 3 - Alpha",
        # 'Development Status :: 5 - Production/Stable',
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Utilities",
    ],
    install_requires=[
        "click>=7.1",
        "pyyaml",
        "crontab",
        "networkx",
        "gitpython",
        "colorama ; platform_system==\"Windows\"",
        "snowflake-connector-python",
        "slack_sdk",
        "boto3",
        "jinja2<3.0.0",
    ],
    entry_points={
        "console_scripts": [
            "dbtease = dbtease.cli:cli",
        ],
    },
)
