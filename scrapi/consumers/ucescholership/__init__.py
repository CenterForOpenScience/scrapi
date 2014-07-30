from scrapi_tools import registry
from consumer import consume, normalize

registry.register('example', consume, normalize)