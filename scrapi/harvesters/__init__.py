"""Importing harvesters imports all file and folders contained in
the harvesters folder. All harvesters should be defined in here.
NOTE: Empty folders in will cause this to break
"""
import os

# Get a list of folders
_, __all__, files = next(os.walk(os.path.dirname(__file__)))

# remove __pycache__ directories
__all__ = [d for d in __all__ if d != '__pycache__']

# Find all .py files that are not init
__all__.extend([
    name[:-3]
    for name in files
    if name[-3:] == '.py'
    and name != '__init__.py'
])

# Import everything in __all__
from . import *
