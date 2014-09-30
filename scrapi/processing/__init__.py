import os

from scrapi import settings
from scrapi.processing.base import BaseProcessor


__all__ = []

for mod in os.listdir(os.path.dirname(__file__)):
    root, ext = os.path.splitext(mod)
    if ext == '.py' and root not in ['__init__', 'base']:
        __all__.append(root)


def process_normalized(raw_doc, normalized):
    for p in settings.NORMALIZED_PROCESSING:
        get_processor.process_normalized(raw_doc, normalized)


def process_raw(raw_doc):
    for p in settings.RAW_PROCESSING:
        get_processor.process_raw(raw_doc)


def get_processor(processor_name):
    for klass in BaseProcessor.__subclasses__():
        if klass.NAME == processor_name:
            return klass()

    raise NotImplementedError('No Processor {}'.format(processor_name))
