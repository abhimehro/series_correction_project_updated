import re

filepath = 'scripts/processor.py'
with open(filepath, 'r') as f:
    content = f.read()

search = """from scripts.discontinuity_utils import (
    DiscontinuityConfig,
    _build_gaps_dataframe,
    _calculate_outlier_replacements,
    _calculate_outlier_z_scores,
    _is_valid_step,
    _perform_interpolation,
    _process_discontinuity,
    _validate_and_convert_time_col,
    _validate_gap_parameters,
    _validate_value_col,
)"""

replace = """from scripts.discontinuity_utils import (
    DiscontinuityConfig,
    _build_gaps_dataframe,
    _calculate_outlier_replacements,
    _calculate_outlier_z_scores,
    _perform_interpolation,
    _process_discontinuity,
    _validate_and_convert_time_col,
    _validate_value_col,
)"""

if search in content:
    content = content.replace(search, replace)
    with open(filepath, 'w') as f:
        f.write(content)
    print("Fixed processor imports.")
else:
    print("Search string not found.")
