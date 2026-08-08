import ast

with open('scripts/tests/test_batch_correction.py', 'r') as f:
    tree = ast.parse(f.read())

helpers = []
tests = []

for node in tree.body:
    if isinstance(node, ast.FunctionDef) and not node.name.startswith('test_'):
        helpers.append(node)
    elif isinstance(node, ast.FunctionDef) and node.name.startswith('test_'):
        tests.append(node)

# To fix "Low Cohesion", we should move the helpers to a separate module, and maybe split the tests.
print(f"Helpers: {len(helpers)}, Tests: {len(tests)}")
