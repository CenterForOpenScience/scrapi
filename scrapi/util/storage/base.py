import os
import json

from scrapi import settings
from scrapi.util import make_dir
from scrapi.util import doc_id_to_path
from scrapi.util import import_consumer


class BaseStorage(object):
    METHOD = None

    # :: Str -> Str -> Nothing
    def _store(string, path):
        raise NotImplementedError('No store method')

    # :: Str -> Bool -> [RawDocument]
    def iter_raws(source, include_normalized=False):
        raise NotImplementedError('No iter raws method')

    # :: Str -> Str
    def get_as_string(self, path):
        raise NotImplementedError('No get as string method')

    # :: Str -> Dict
    def get_as_json(self, path):
        return json.loads(self.get_as_string(path))

    # :: Str -> Str
    def _build_path(self, raw_doc):
        path = [
            settings.ARCHIVE_DIRECTORY,
            raw_doc.get('source'),
            doc_id_to_path(raw_doc.get('doc_id')),
            raw_doc.get('timestamp')
        ]

        path = os.path.join(*path)
        make_dir(path)

        return path

    # :: NormalizedDocument -> Nothing
    def store_normalized(self, raw_doc, document):
        path = self._build_path(raw_doc)
        path = os.path.join(path, 'normalized.json')

        self._store(json.dumps(document.attributes), path)

    # :: RawDocument -> Nothing
    def store_raw(self, document):
        manifest = import_consumer(document.get('source'))
        doc_name = 'raw.{}'.format(manifest['file_type'])

        path = self._build_path(document)
        path = os.path.join(path, doc_name)

        self._store(document.get('doc'), path)

        manifest_fields = {
        }

    # :: RawDocument -> Dict -> Nothing
    def update_manifest(self, path, fields):
        path = os.path.join(path, 'manifest.json')
        manifest = self.get_as_json(path)
        manifest.update(fields)
        self._store(path, json.dumps(manifest))
