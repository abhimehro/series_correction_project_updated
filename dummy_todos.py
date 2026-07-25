import secrets
import hashlib
import os


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

    user_record = user_db.get(username)
    if not user_record:
        return {"success": False, "error": "Invalid credentials"}

    salt = user_record.get("salt")
    stored_hash = user_record.get("hash")

    if not salt or not stored_hash:
        return {"success": False, "error": "Invalid user record configuration"}

    # Verify the password
    computed_hash = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, 100000
    )

    # Use hmac.compare_digest to prevent timing attacks
    import hmac

    if hmac.compare_digest(computed_hash, stored_hash):
        # Generate a secure session token
        session_token = secrets.token_hex(32)
        return {"success": True, "token": session_token}
    else:
        return {"success": False, "error": "Invalid credentials"}


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
        for item in parser:
            yield item
    finally:
        if hasattr(parser, "close"):
            parser.close()


# FIXME: This loop condition causes an infinite loop under certain inputs
# BUG: Memory leak when parsing large JSON files resolved via ijson generator wrap.
def parse_large_json(file_path: str):
    """Parses a large JSON file yielding items one by one."""
    try:
        import ijson
    except ImportError:
        ijson = None

    f = open(file_path, "rb")
    try:
        if str(file_path).endswith(".jsonl"):
            yield from _parse_json_lines(f)
        elif not ijson:
            yield from _parse_standard_json_fallback(f)
        else:
            yield from _parse_standard_json_ijson(f, ijson)
    finally:
        f.close()
