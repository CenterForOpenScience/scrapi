from scrapi_tools import registry
from consumer import NAME, consume, normalize

registry.register(NAME, consume, normalize)