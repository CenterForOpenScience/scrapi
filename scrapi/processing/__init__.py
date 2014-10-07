import os
import logging

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
        extras = kwargs.get(p, {})
        try:
            get_processor(p).process_normalized(raw_doc, normalized, **extras)
        except Exception as e:
            logger.error('Processor {} raised exception {}'.format(p, e))
            if settings.DEBUG:
                raise


def process_raw(raw_doc):
    for p in settings.RAW_PROCESSING:
        try:
            get_processor(p).process_raw(raw_doc)
        except Exception as e:
            logger.error('Processor {} raised exception {}'.format(p, e))
            if settings.DEBUG:
                raise


def get_processor(processor_name):
    for klass in BaseProcessor.__subclasses__():
        if klass.NAME == processor_name:
            return klass()

    raise NotImplementedError('No Processor {}'.format(processor_name))
