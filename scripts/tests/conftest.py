# conftest.py to ensure project root is on sys.path for imports
import os
import sys

# Add project root to path for pytest
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
