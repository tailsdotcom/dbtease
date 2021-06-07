"""Local Filestore Class."""

import logging
import os
import os.path

from dbtease.filestores.base import Filestore


class LocalFilestore(Filestore):
    """Local Filestore Connection."""

    def __init__(self, path):
        self._local_path = os.path.expanduser(path)

    def check_access(self):
        """Test that we can write to the dest folder."""
        try:
            self._upload_filestr(".testfile", "test")
        except Exception as err:
            logging.error(err)
            return False
        return True

    def _upload_filestr(self, fname, content):
        # Make folder if it doesn't exist
        if not os.path.exists(self._local_path):
            os.makedirs(self._local_path)
        with open(
            os.path.join(self._local_path, fname), "w", encoding="utf8"
        ) as dest_file:
            dest_file.write(content)

    def upload_files(self, *paths: str):
        for path in paths:
            _, fname = os.path.split(path)
            with open(path, encoding="utf8") as stash_file:
                content = stash_file.read()
                self._upload_filestr(fname, content)
