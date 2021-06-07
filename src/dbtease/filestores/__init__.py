"""Filestore connections.

Eventually we should do something smart here to only load the
ones we can given the installed libraries.

That's a job for another day.
"""

from dbtease.filestores.local import LocalFilestore
from dbtease.filestores.aws import S3Filestore

_filestore_options = {
    "local": LocalFilestore,
    "s3": S3Filestore,
}


def get_filestore_from_config(filestore_config, **kwargs):
    # Get the first config type in the config
    if not filestore_config:
        return None
    filestore_type, config = filestore_config.popitem()
    if filestore_type not in _filestore_options:
        raise ValueError(
            f"Filestore of type {filestore_type} are not supported yet in dbtease."
        )
    return _filestore_options[filestore_type].from_dict(config, **kwargs)
