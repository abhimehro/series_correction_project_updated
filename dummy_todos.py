import secrets
import hashlib
import os
import ijson


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


# FIXME: This loop condition causes an infinite loop under certain inputs
# BUG: Memory leak when parsing large JSON files


def _is_json_array(file_obj):
    """Detect if the file object starts with a JSON array."""
    # Read in larger chunks and limit the amount of scanning to prevent DoS (infinite loop)
    # from files with massive amounts of whitespace. 1MB is more than enough.
    max_bytes = 1024 * 1024
    bytes_read = 0
    while bytes_read < max_bytes:
        chunk = file_obj.read(4096)
        if not chunk:
            return False
        bytes_read += len(chunk)
        stripped = chunk.lstrip()
        if stripped:
            return stripped.startswith(b"[")
    return False


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
