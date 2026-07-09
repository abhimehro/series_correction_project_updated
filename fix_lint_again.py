import re

filepath = 'scripts/processor.py'
with open(filepath, 'r') as f:
    content = f.read()

# Fix the import issue
search_import = """from scripts.discontinuity_utils import (
    DiscontinuityConfig,
    _build_gaps_dataframe,
    _calculate_outlier_replacements,
    _calculate_outlier_z_scores,
    _perform_interpolation,
    _process_discontinuity,
    _validate_and_convert_time_col,
    _validate_value_col,
)"""

replace_import = """from scripts.discontinuity_utils import (
    DiscontinuityConfig,
    _build_gaps_dataframe,
    _calculate_outlier_replacements,
    _calculate_outlier_z_scores,
    _perform_interpolation,
    _process_discontinuity,
    _validate_and_convert_time_col,
    _validate_value_col,
)"""

content = content.replace(search_import, replace_import)

with open(filepath, 'w') as f:
    f.write(content)
