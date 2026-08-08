import ast

with open('scripts/tests/test_batch_correction.py', 'r') as f:
    source = f.read()

tree = ast.parse(source)

imports = []
fixtures = []
other_helpers = []
test_batch_happy = []
test_batch_error = []
test_other = []

for node in tree.body:
    if isinstance(node, (ast.Import, ast.ImportFrom)):
        imports.append(node)
    elif isinstance(node, ast.FunctionDef):
        is_fixture = any(
            (isinstance(dec, ast.Attribute) and dec.attr == 'fixture') or
            (isinstance(dec, ast.Name) and dec.id == 'fixture') or
            (isinstance(dec, ast.Call) and getattr(dec.func, 'attr', '') == 'fixture')
            for dec in node.decorator_list
        )
        if is_fixture:
            fixtures.append(node)
        elif not node.name.startswith('test_'):
            other_helpers.append(node)
        elif 'happy' in node.name or 'dry_run' in node.name:
            test_batch_happy.append(node)
        elif 'error' in node.name or 'not_found' in node.name or 'invalid' in node.name or 'exception' in node.name:
            test_batch_error.append(node)
        else:
            test_other.append(node)
    else:
        imports.append(node) # globals etc

# We will rewrite the AST to group tests into classes.
class_happy = ast.ClassDef(
    name='TestBatchCorrectionHappyPath',
    bases=[],
    keywords=[],
    body=test_batch_happy,
    decorator_list=[]
)

class_error = ast.ClassDef(
    name='TestBatchCorrectionErrors',
    bases=[],
    keywords=[],
    body=test_batch_error,
    decorator_list=[]
)

class_other = ast.ClassDef(
    name='TestBatchCorrectionOther',
    bases=[],
    keywords=[],
    body=test_other,
    decorator_list=[]
)

# We need to add 'self' as the first argument to all methods in these classes
for cls in [class_happy, class_error, class_other]:
    for method in cls.body:
        if isinstance(method, ast.FunctionDef):
            method.args.args.insert(0, ast.arg(arg='self', annotation=None))
            # Also modify any calls to other class methods if needed, but in pytest tests usually don't call each other.

new_body = imports + other_helpers + fixtures + [class_happy, class_error, class_other]
tree.body = new_body

ast.fix_missing_locations(tree)
new_source = ast.unparse(tree)

with open('scripts/tests/test_batch_correction.py', 'w') as f:
    f.write(new_source)
