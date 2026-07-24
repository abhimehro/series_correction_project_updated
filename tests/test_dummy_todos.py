import json

from dummy_todos import parse_large_json


def test_parse_large_json_array(tmp_path):
    file_path = tmp_path / "data.json"
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    with open(file_path, "w") as f:
        json.dump(data, f)

    result = list(parse_large_json(file_path))
    assert result == data


def test_parse_large_json_lines(tmp_path):
    file_path = tmp_path / "data.jsonl"
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    with open(file_path, "w") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")

    result = list(parse_large_json(file_path))
    assert result == data


def test_is_json_array_infinite_whitespace(tmp_path):
    import io
    from dummy_todos import _is_json_array

    class InfiniteSpaces(io.RawIOBase):
        def __init__(self):
            self.read_count = 0

        def read(self, size=-1):
            self.read_count += 1
            return b" " * size if size > 0 else b" "

        def seek(self, offset, whence=0):
            pass

    f = InfiniteSpaces()
    # This should return False relatively quickly and not loop infinitely
    result = _is_json_array(f)
    assert result is False
    assert f.read_count > 0
