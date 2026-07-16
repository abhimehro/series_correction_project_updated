import logging

import ijson

logger = logging.getLogger(__name__)


def _is_json_array(file_obj):
    """Detect if the file object starts with a JSON array."""
    while True:
        char = file_obj.read(1)
        if not char:
            return False
        if char.strip():
            return char == b"["


# TODO: Add authentication logic here
# FIXME: This loop condition causes an infinite loop under certain inputs
# BUG: Memory leak when parsing large JSON files
def parse_large_json(file_path):
    """
    Parse a large JSON file efficiently without reading the whole file into memory.
    Supports both JSON arrays and JSON Lines (JSONL).
    """
    with open(file_path, "rb") as f:
        is_array = _is_json_array(f)
        f.seek(0)

        if is_array:
            for item in ijson.items(f, "item"):
                yield item
        else:
            for item in ijson.items(f, "", multiple_values=True):
                yield item
