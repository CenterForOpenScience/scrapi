import os
import json

from scrapi import settings
from scrapi.util import make_dir
from scrapi.util import safe_filename


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
            safe_filename(x)
            for x in [
                raw_doc.get('source'),
                raw_doc.get('docID'),
                raw_doc.get('timestamp')
            ]
        ]

        path = os.path.join(settings.ARCHIVE_DIRECTORY, *path)
        make_dir(path)

        return path

    # :: NormalizedDocument -> Nothing
    def store_normalized(self, raw_doc, document, overwrite=False):
        path = self._build_path(raw_doc)
        manifest = settings.MANIFESTS[document.get('source')]
        manifest_update = {
            'normalizeVersion': manifest['version']
        }

        self.update_manifest(path, manifest_update)

        path = os.path.join(path, 'normalized.json')

        self._store(json.dumps(document.attributes), path, overwrite=overwrite)

    # :: RawDocument -> Nothing
    def store_raw(self, document):
        manifest = settings.MANIFESTS[document.get('source')]
        doc_name = 'raw.{}'.format(manifest['fileFormat'])
        path = self._build_path(document)
        manifest = {
            'timestamp': document.get('timestamp'),
            'source': document.get('source'),
            'consumeVersion': manifest['version']
        }

        self.update_manifest(path, manifest)

        path = os.path.join(path, doc_name)

        self._store(document.get('doc'), path)

    # :: RawDocument -> Dict -> Nothing
    def update_manifest(self, path, fields):
        path = os.path.join(path, 'manifest.json')
        try:
            manifest = self.get_as_json(path)
        except Exception:  # TODO Make this more specific
            manifest = {}

        manifest.update(fields)
        self._store(json.dumps(manifest), path, overwrite=True)
