from .defaults import *

try:
    from .local import *
except ImportError as error:
    raise ImportError("No local.py settings file found.")
