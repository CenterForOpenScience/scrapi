from scrapi.util.storage import store
from scrapi.processing.base import BaseProcessor


class StorageProcessor(BaseProcessor):
    NAME = 'storage'

    def process_normalized(raw_doc, normalized):
        store.store_normalized(raw_doc, normalized)

    def process_raw(raw_doc):
        store.store_raw(raw_doc)
