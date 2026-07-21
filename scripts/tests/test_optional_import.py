import logging
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


def test_optional_import_syntax_error(monkeypatch):
    def mock_import(*args, **kwargs):
        raise SyntaxError("Mocked SyntaxError")

    monkeypatch.setattr("builtins.__import__", mock_import)

    with pytest.raises(SyntaxError):
        _optional_import("some_module", "fallback")
