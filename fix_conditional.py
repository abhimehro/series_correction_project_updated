with open('scripts/processor.py', 'r') as f:
    content = f.read()

import re

# We will use regex to find the multiline string and replace it
# because black formatted it onto multiple lines
pattern = r"    # Map back to DataFrame index if it's not a standard RangeIndex.*?gap_indices = data\.index\[gap_indices_np\]\.tolist\(\)"
replacement = r"""    # Map back to original DataFrame index
    gap_indices = data.index[gap_indices_np].tolist()"""

if re.search(pattern, content, re.DOTALL):
    content = re.sub(pattern, replacement, content, flags=re.DOTALL)
    with open('scripts/processor.py', 'w') as f:
        f.write(content)
    print("Replaced!")
else:
    print("String not found!")
