import logging
logger = logging.getLogger(__name__)

from .defaults import *

try:
    from .local import *
except ImportError as error:
    logger.warn("No api local.py settings file found. Try running $ cp api/api/settings/local-dist.py api/api/settings/local.py. Defaulting to scrapi/settings/local.py")
    from scrapi.settings.local import *
