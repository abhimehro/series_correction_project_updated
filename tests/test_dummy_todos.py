import json

from dummy_todos import authenticate, generate_salt_and_hash, parse_large_json


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


def test_authentication():
    # Setup
    password = "secure_password_123"
    salt, hash_val = generate_salt_and_hash(password)
    user_db = {"alice": {"salt": salt, "hash": hash_val}}

    # Test successful authentication
    res = authenticate("alice", "secure_password_123", user_db)
    assert res["success"] is True
    assert "token" in res

    # Test wrong password
    res = authenticate("alice", "wrong_password", user_db)
    assert res["success"] is False
    assert res["error"] == "Invalid credentials"

    # Test wrong username
    res = authenticate("bob", "secure_password_123", user_db)
    assert res["success"] is False
    assert res["error"] == "Invalid credentials"

    # Test missing inputs
    res = authenticate("", "secure_password_123", user_db)
    assert res["success"] is False
    assert res["error"] == "Username and password are required"


def test_is_json_array_infinite_loop(tmp_path):
    from dummy_todos import _is_json_array

    # Test file with only whitespace, previously would infinite loop
    file_path = tmp_path / "whitespace.txt"
    with open(file_path, "wb") as f:
        # 2000 spaces
        f.write(b" " * 2000)

    with open(file_path, "rb") as f:
        # Should return False after hitting the maximum read limit
        assert _is_json_array(f) is False
