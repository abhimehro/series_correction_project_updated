import os
import sys

# Add project root to path
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, PROJECT_ROOT)


def test_fix_output_imports():
    """Verify that fix_output.py imports are correct and don't fail."""
    # Temporarily remove pandas to see if fix_output.py successfully imports it
    if "pandas" in sys.modules:
        del sys.modules["pandas"]

    # We don't want to actually run the script's logic since it executes on load
    # (it does not have a if __name__ == '__main__': block).
    # But since it runs on import, we will patch out the print/file ops if possible,
    # or just parse the AST to ensure 'pandas' is imported.
    import ast

    script_path = os.path.join(PROJECT_ROOT, "scripts", "fix_output.py")
    with open(script_path, "r") as f:
        tree = ast.parse(f.read())

    imports = [
        node.names[0].name for node in ast.walk(tree) if isinstance(node, ast.Import)
    ]
    assert "pandas" in imports, "pandas is not imported in fix_output.py"
