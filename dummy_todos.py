import hashlib
import hmac
import os
import secrets


def generate_salt_and_hash(password: str) -> tuple[bytes, bytes]:
    """Generates a salt and hash for a given password."""
    salt = os.urandom(16)
    password_hash = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, 100000
    )
    return salt, password_hash


def authenticate(username: str, password: str, user_db: dict) -> dict:
    """
    Authenticates a user against a provided database.

    Args:
        username: The user's username.
        password: The user's plaintext password.
        user_db: A dictionary mapping usernames to dictionaries containing 'salt' and 'hash'.

    Returns:
        A dictionary containing the success status and a session token or error message.
    """
    if not username or not password:
        return {"success": False, "error": "Username and password are required"}

    # SECURITY: Use a dummy salt to ensure PBKDF2 executes even for non-existent users,
    # preventing user enumeration via timing attacks.
    dummy_salt = b"\x00" * 16
    user_record = user_db.get(username)

    if user_record:
        salt = user_record.get("salt")
        stored_hash = user_record.get("hash")
    else:
        salt = None
        stored_hash = None

    if not salt or not stored_hash:
        salt = dummy_salt
        stored_hash = None

    # Always compute the hash to mitigate timing side-channels
    computed_hash = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, 100000
    )

    if stored_hash and hmac.compare_digest(computed_hash, stored_hash):
        session_token = secrets.token_hex(32)
        return {"success": True, "token": session_token}

    return {"success": False, "error": "Invalid credentials"}


def _is_json_array(file_obj):
    """Detect if the file object starts with a JSON array."""
    while char := file_obj.read(1):
        if char.strip():
            return char == b"["
    return False


def _parse_json_lines(file_obj):
    import json

    for line in file_obj:
        yield json.loads(line)


def _parse_standard_json_fallback(file_obj):
    import json

    yield from json.load(file_obj)


def _parse_standard_json_ijson(file_obj, ijson_module):
    parser = ijson_module.items(file_obj, "item")
    try:
        yield from parser
    finally:
        if hasattr(parser, "close"):
            parser.close()


def parse_large_json(file_path: str):
    """Parses a large JSON file yielding items one by one."""
    try:
        import ijson
    except ImportError:
        ijson = None

    with open(file_path, "rb") as f:
        if str(file_path).endswith(".jsonl"):
            yield from _parse_json_lines(f)
        elif not ijson:
            yield from _parse_standard_json_fallback(f)
        else:
            is_array = _is_json_array(f)
            f.seek(0)
            if is_array:
                yield from _parse_standard_json_ijson(f, ijson)
            else:
                yield from _parse_standard_json_fallback(f)
