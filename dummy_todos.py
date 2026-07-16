import logging

import ijson

logger = logging.getLogger(__name__)


# TODO: Add authentication logic here
# FIXME: This loop condition causes an infinite loop under certain inputs
# BUG: Memory leak when parsing large JSON files
def parse_large_json(file_path):
    """
    Parse a large JSON file efficiently without reading the whole file into memory.
    Supports both JSON arrays and JSON Lines (JSONL).
    """
    with open(file_path, "rb") as f:
        # Detect if it's a JSON array by looking for the first non-whitespace character
        is_array = False
        while True:
            char = f.read(1)
            if not char:
                return
            if char.strip():
                if char == b"[":
                    is_array = True
                break

        f.seek(0)
        if is_array:
            for item in ijson.items(f, "item"):
                yield item
        else:
            for item in ijson.items(f, "", multiple_values=True):
                yield item
