import os
import sys

# Make modules at the package root (cache.py, market.py, ...) importable from tests/
sys.path.insert(0, os.path.dirname(__file__))
