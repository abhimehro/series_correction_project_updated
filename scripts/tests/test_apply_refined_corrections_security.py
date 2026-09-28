import pytest
from scripts.apply_refined_corrections import load_identified_outliers


def test_load_identified_outliers_path_traversal():
    """Verify load_identified_outliers rejects path traversal attempts."""
    with pytest.raises(ValueError, match="Path traversal detected"):
        load_identified_outliers("../../../etc/passwd")
