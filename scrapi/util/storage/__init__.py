import os

from scrapi import settings
from scrapi.util.storage.base import BaseStorage


__all__ = []


for mod in os.listdir(os.path.dirname(__file__)):
    root, ext = os.path.splitext(mod)
    if ext == '.py' and root not in ['__init__', 'base']:
        __all__.append(root)


from . import *


def _get_storage(method):
    for klass in BaseStorage.__subclasses__():
        if method == klass.METHOD:
            return klass()
    raise NotImplementedError('Missing storage method "{}"'.format(method))

store = _get_storage(settings.STORAGE_METHOD)
