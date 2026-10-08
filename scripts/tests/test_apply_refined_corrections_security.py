import os
import pytest

from scripts.apply_refined_corrections import (
    build_raw_file_map,
    load_identified_outliers,
)


def test_load_identified_outliers_path_traversal():
    traversal_path = os.path.join("..", "..", "etc", "passwd")
    with pytest.raises(ValueError, match="Path traversal detected"):
        load_identified_outliers(traversal_path)


def test_build_raw_file_map_path_traversal():
    traversal_path = os.path.join("..", "..", "etc")
    with pytest.raises(ValueError, match="Path traversal detected"):
        build_raw_file_map(traversal_path)
