from scrapi.util.storage import store
from scrapi.processing.base import BaseProcessor


class StorageProcessor(BaseProcessor):
    NAME = 'storage'

    def process_normalized(self, raw_doc, normalized, overwrite=False, is_push=False):
        store.store_normalized(raw_doc, normalized, overwrite=overwrite, is_push=is_push)

    def process_raw(self, raw_doc, is_push=False):
        store.store_raw(raw_doc, is_push=is_push)
