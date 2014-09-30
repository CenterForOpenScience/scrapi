from scrapi.util.storage import store
from scrapi.processing.base import BaseProcessor


class StorageProcessor(BaseProcessor):
    NAME = 'storage'

    def process_normalized(self, raw_doc, normalized):
        store.store_normalized(raw_doc, normalized)

    def process_raw(self, raw_doc):
        store.store_raw(raw_doc)
