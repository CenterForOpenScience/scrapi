import os

from collections import namedtuple
from celery.signals import worker_process_init

from scrapi import settings
from scrapi.processing.base import BaseProcessor

DocumentTuple = namedtuple('Document', ['raw', 'normalized'])

__all__ = []
for mod in os.listdir(os.path.dirname(__file__)):
    root, ext = os.path.splitext(mod)
    if ext == '.py' and root not in ['__init__', 'base']:
        __all__.append(root)

from . import *


def get_processor(processor_name):
    for klass in BaseProcessor.__subclasses__():
        if klass.NAME == processor_name:
            return klass()
    raise NotImplementedError('No Processor {}'.format(processor_name))


def process_normalized(raw_doc, normalized, kwargs):
    ''' kwargs is a dictiorary of kwargs.
        keyed by the processor name
        Exists so that when we run check archive we
        specifiy that it's ok to overrite certain files
    '''
    for p in settings.NORMALIZED_PROCESSING:
        extras = kwargs.get(p, {})
        get_processor(p).process_normalized(raw_doc, normalized, **extras)


def process_raw(raw_doc, kwargs):
    for p in settings.RAW_PROCESSING:
        extras = kwargs.get(p, {})
        get_processor(p).process_raw(raw_doc, **extras)


HarvesterResponse = get_processor(settings.RESPONSE_PROCESSOR).HarvesterResponseModel

all_processors = map(get_processor, list(set(
    settings.NORMALIZED_PROCESSING +
    settings.RAW_PROCESSING +
    [settings.RESPONSE_PROCESSOR]
)))

for processor in all_processors:
    processor.manager.setup()
    worker_process_init.connect(processor.manager.celery_setup)
