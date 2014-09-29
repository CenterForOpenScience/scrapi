import os

from scrapi.util.storage.base import BaseStorage


class DiskStorage(BaseStorage):
    METHOD = 'disk'

    def store(document, filepath):
        # Never overwrite other files
        if os.path.exists(filepath):
            raise Exception('"{}" already exists.'.format(filepath))

        with open(filepath, 'w') as docfile:
            docfile.write(document)
