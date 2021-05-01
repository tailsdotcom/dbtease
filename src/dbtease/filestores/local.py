"""Local Filestore Class."""

import os.path
import logging

from dbtease.filestores.base import Filestore


class LocalFilestore(Filestore):
    """Local Filestore Connection."""

    def __init__(self, path):
        self._local_path = os.path.expanduser(path)
    
    def upload_files(self, *paths: str):
        for path in paths:
            _, fname = os.path.split(path)
            with open(path, encoding="utf8") as stash_file:
                content = stash_file.read()
                with open(
                    os.path.join(self._local_path, fname),
                    "w",
                    encoding="utf8"
                ) as dest_file:
                    dest_file.write(content)
