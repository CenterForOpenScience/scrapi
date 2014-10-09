import os
import logging

from scrapi import events
from scrapi import settings
from scrapi.processing.base import BaseProcessor


logger = logging.getLogger(__name__)


__all__ = []

for mod in os.listdir(os.path.dirname(__file__)):
    root, ext = os.path.splitext(mod)
    if ext == '.py' and root not in ['__init__', 'base']:
        __all__.append(root)

from . import *


def process_normalized(raw_doc, normalized, kwargs):
    for p in settings.NORMALIZED_PROCESSING:
        _normalized_event(events.STARTED, p, raw_doc)

        extras = kwargs.get(p, {})

        try:
            get_processor(p).process_normalized(raw_doc, normalized, **extras)
        except Exception as e:
            _normalized_event(events.FAILED, p, raw_doc, exception=str(e))
            logger.error('Processor {} raised exception {}'.format(p, e))
            if settings.DEBUG:
                raise
        else:
            _normalized_event(events.COMPLETED, p, raw_doc)


def process_raw(raw_doc):
    for p in settings.RAW_PROCESSING:
        _raw_event(events.STARTED, p, raw_doc)

        try:
            get_processor(p).process_raw(raw_doc)
        except Exception as e:
            logger.error('Processor {} raised exception {}'.format(p, e))
            _raw_event(events.FAILED, p, raw_doc, exception=str(e))
            if settings.DEBUG:
                raise
        else:
            _raw_event(events.COMPLETED, p, raw_doc)


def get_processor(processor_name):
    for klass in BaseProcessor.__subclasses__():
        if klass.NAME == processor_name:
            return klass()

    raise NotImplementedError('No Processor {}'.format(processor_name))


def _normalized_event(status, processor, raw, **kwargs):
    __processing_event(status, processor, raw,
        _index='normalized.{}'.format(processor), **kwargs)


def _raw_event(status, processor, raw, **kwargs):
    __processing_event(status, processor, raw,
        _index='raw.{}'.format(processor), **kwargs)


def __processing_event(status, processor_name, raw_doc, **kwargs):
    events.dispatch(events.PROCESSING, status, docID=raw_doc['docID'], **kwargs)
