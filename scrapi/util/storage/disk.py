import os

from scrapi import settings
from scrapi.util.storage.base import BaseStorage


class DiskStorage(BaseStorage):
    METHOD = 'disk'

    def _store(self, string, filepath, overwrite=False):
        # Never overwrite other files
        if os.path.exists(filepath) and not overwrite:
            raise Exception('"{}" already exists.'.format(filepath))

        with open(filepath, 'w') as docfile:
            docfile.write(string)

    def get_as_string(self, path):
        with open(path)as f:
            return f.read()

    # :: Str -> Bool -> [RawDocument]
    def iter_raws(self, source, include_normalized=False):
        src_dir = os.path.join(settings.ARCHIVE_DIRECTORY, source)

        for dirname, dirnames, filenames in os.walk(src_dir):
            if 'normalized.json' not in filenames or include_normalized:
                for filename in filenames:
                    if 'raw' in filename:
                        yield os.path.join(dirname, filename)
