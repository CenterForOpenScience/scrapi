from .defaults import *

try:
    from .local import *
except ImportError as error:
    raise ImportError("No api local.py settings file found. Try running $ cp api/api/settings/local-dist.py api/api/settings/local.py")
