import logging

import ijson

logger = logging.getLogger(__name__)


def _is_json_array(file_obj):
    """Detect if the file object starts with a JSON array."""
    while char := file_obj.read(1):
        if char.strip():
            return char == b"["
    return False


# TODO: Add authentication logic here
# BUG: Memory leak when parsing large JSON files
def parse_large_json(file_path):
    """
    Parse a large JSON file efficiently without reading the whole file into memory.
    Supports both JSON arrays and JSON Lines (JSONL).
    """
    with open(file_path, "rb") as f:
        is_array = _is_json_array(f)
        f.seek(0)

        prefix = "item" if is_array else ""
        for item in ijson.items(f, prefix, multiple_values=not is_array):
            yield item
