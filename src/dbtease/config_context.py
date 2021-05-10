import os
import json
import os.path
import shutil
import logging
from contextlib import contextmanager

logger = logging.getLogger("dbtease.config_context")


class ConfigContext:
    """Context manager for handling context files."""

    def __init__(self, file_dict=None, config_path=".dbtease"):
        self.config_path = config_path
        self.file_dict = file_dict or {}

    def __enter__(self):
        """Set up the config environment."""
        # Make folder if not exists
        if not os.path.exists(self.config_path):
            os.makedirs(self.config_path)
        # Populate the folder
        self._persist_file_dict(self.file_dict)
        logger.debug("Using config path: %r", self.config_path)
        return self

    def __exit__(self, type, value, traceback):
        """Clean up."""
        if os.path.exists(self.config_path):
            logger.debug("Cleaning config path...")
            shutil.rmtree(self.config_path)
        pass

    def _persist_file_dict(self, file_dict):
        # Populate the folder
        for fname in file_dict:
            with open(
                os.path.join(self.config_path, fname), "w", encoding="utf8"
            ) as config_file:
                config_file.write(file_dict[fname])

    def update_files(self, file_dict):
        # update the self record
        self.file_dict.update(file_dict)
        # Persist the new changes
        self._persist_file_dict(file_dict)

    @contextmanager
    def patch_files(self, file_dict):
        """Temporarily patch files in context as a context manager."""
        # Overwrite with new files
        self._persist_file_dict(file_dict)
        try:
            yield
        finally:
            # Restore original files.
            self._persist_file_dict(
                {fname: self.file_dict[fname] for fname in file_dict}
            )

    def stash_files(self, *paths):
        file_dict = {}
        # read the files
        for path in paths:
            _, fname = os.path.split(path)
            with open(path, encoding="utf8") as stash_file:
                file_dict[fname] = stash_file.read()
        # Add them to the session
        self.update_files(file_dict)

    def read_file(self, fname):
        with open(os.path.join(self.config_path, fname), encoding="utf8") as read_file:
            content = read_file.read()
        return content

    def __str__(self):
        """Just return the path if we ever make a string of this."""
        return self.config_path

    @staticmethod
    def compare_manifests(manifest_a, manifest_b):
        manifest_obj_a = json.loads(manifest_a)
        manifest_obj_b = json.loads(manifest_b)
        if manifest_obj_a == manifest_obj_b:
            # Simple same
            return True
        else:
            for key in manifest_obj_a.keys():
                if key in ("metadata", "macros"):
                    continue
                # Compare remaining keys to see if anything meaningful has changed.
                is_same = manifest_obj_a[key] == manifest_obj_b[key]
                if not is_same:
                    return False
        return True
