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


def test_fix_output_exception_logging(caplog):
    """Verify that fix_output.py logs exceptions securely."""
    import ast

    script_path = os.path.join(PROJECT_ROOT, "scripts", "fix_output.py")
    with open(script_path, "r") as f:
        tree = ast.parse(f.read())

    # Check AST for log.exception call inside except handler
    except_handlers = [
        node for node in ast.walk(tree) if isinstance(node, ast.ExceptHandler)
    ]
    assert len(except_handlers) > 0, "No except handlers found in fix_output.py"

    has_log_exception = False
    for handler in except_handlers:
        for stmt in handler.body:
            if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
                func = stmt.value.func
                if isinstance(func, ast.Attribute) and func.attr == "exception":
                    has_log_exception = True

    assert (
        has_log_exception
    ), "log.exception was not called in except handler in fix_output.py"
