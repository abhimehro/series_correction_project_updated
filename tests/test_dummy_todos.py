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
