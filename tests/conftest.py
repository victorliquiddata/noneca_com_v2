# tests/conftest.py

import os
import sys

# make the project root importable (so both src/ and config/ work)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
