import importlib
import logging
from unittest.mock import patch

import pytest

from scripts.batch_correction import _optional_import


def test_optional_import_success():
    module = _optional_import("os", "fallback")
    import os

    assert module is os


def test_optional_import_module_not_found(caplog):
    with caplog.at_level(logging.INFO):
        module = _optional_import("non_existent_module_12345", "My fallback message")
    assert module is None
    assert "My fallback message" in caplog.text


def test_optional_import_import_error(monkeypatch, caplog):
    def mock_import(*args, **kwargs):
        raise ImportError("Mocked ImportError")

    monkeypatch.setattr("builtins.__import__", mock_import)

    with caplog.at_level(logging.ERROR):
        module = _optional_import("some_module", "fallback")

    assert module is None
    assert "Import error while loading some_module" in caplog.text


def test_optional_import_import_error_with_importlib(caplog):
    """Test ImportError handling with importlib mocking and verify exception logging."""
    target_module = "scripts.loaders"
    error_message = "Mocked import failure via importlib"

    def fake_import(name, *args, **kwargs):
        if name == target_module:
            raise ImportError(error_message)
        return importlib.__import__(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=fake_import):
        with caplog.at_level(logging.ERROR):
            module = _optional_import(target_module, "Fallback config loader message")

    assert module is None
    assert f"Import error while loading {target_module}" in caplog.text
    assert error_message in caplog.text


def test_optional_import_syntax_error(monkeypatch):
    def mock_import(*args, **kwargs):
        raise SyntaxError("Mocked SyntaxError")

    monkeypatch.setattr("builtins.__import__", mock_import)

    with pytest.raises(SyntaxError):
        _optional_import("some_module", "fallback")
