---
name: gitnexus-area-scripts
description: "Skill for the Scripts area of series_correction_project_updated. 102 symbols across 14 files."
---

# Scripts

102 symbols | 14 files | Cohesion: 81%

## When to Use

- Working with code in `scripts/`
- Understanding how process_data, test_process_data_time_col_parsing_failure,
  apply_level_shift_correction work
- Modifying scripts-related functionality

## Key Files

| File                                             | Symbols                                                                                                                                                                                                                                                          |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/spreadsheet_safety.py`                  | _find_null_byte_in_index, _find_null_in_categorical_index, _find_null_in_index_name, _find_null_in_multiindex, _find_null_in_multiindex_level (+26)                                                                                                              |
| `scripts/export_comparison_sheets.py`            | _find_series_file_match, _find_year_file_match, find_matching_raw_file, _load_and_merge_data, _rename_raw_columns (+8)                                                                                                                                           |
| `scripts/apply_refined_corrections.py`           | _calculate_and_apply_shift, apply_level_shift_correction, find_year_files, has_sensor_window, output_file_name (+5)                                                                                                                                              |
| `scripts/discontinuity_utils.py`                 | _auto_detect_value_col, _convert_time_col_to_numeric, _process_discontinuity, _validate_and_convert_time_col, _validate_value_col (+4)                                                                                                                           |
| `scripts/batch_correction.py`                    | _build_rm_to_sensors_map, _determine_series_to_process, _get_explicit_series, _get_series_from_all, _load_raw_data (+4)                                                                                                                                          |
| `scripts/processor.py`                           | _get_processing_steps, _merge_config, process_data, correct_gaps, correct_outliers                                                                                                                                                                               |
| `scripts/tests/test_batch_correction.py`         | test_determine_series_to_process_all_fallback, test_determine_series_to_process_all_fallback_with_river_miles, test_determine_series_to_process_explicit_invalid_value, test_determine_series_to_process_invalid_sensor_id_in_map, test_load_raw_data_empty_file |
| `scripts/tests/test_spreadsheet_safety.py`       | test_escape_spreadsheet_formula, test_escape_spreadsheet_formula_is_idempotent, test_escape_spreadsheet_formula_prefixes, test_write_csv_safely_neutralizes_payloads, test_write_csv_safely_sanitizes_custom_header_and_index_label                              |
| `scripts/tests/test_export_comparison_sheets.py` | test_find_matching_raw_file_no_match, test_find_matching_raw_file_series_format, test_find_matching_raw_file_year_format, test_process_single_file_escapes_malicious_comment                                                                                     |
| `scripts/generate_overview_table.py`             | _print_results, _process_log_data, _process_outlier_log, _safe_round                                                                                                                                                                                             |

## Entry Points

Start here when exploring this area:

- **`process_data`** (Function) — `scripts/processor.py:539`
- **`test_process_data_time_col_parsing_failure`** (Function) —
  `scripts/tests/test_processor.py:55`
- **`apply_level_shift_correction`** (Function) —
  `scripts/apply_refined_corrections.py:196`
- **`find_year_files`** (Function) — `scripts/apply_refined_corrections.py:140`
- **`has_sensor_window`** (Function) —
  `scripts/apply_refined_corrections.py:153`

## Key Symbols

| Symbol                                                           | Type     | File                                              | Line |
| ---------------------------------------------------------------- | -------- | ------------------------------------------------- | ---- |
| `process_data`                                                   | Function | `scripts/processor.py`                            | 539  |
| `test_process_data_time_col_parsing_failure`                     | Function | `scripts/tests/test_processor.py`                 | 55   |
| `apply_level_shift_correction`                                   | Function | `scripts/apply_refined_corrections.py`            | 196  |
| `find_year_files`                                                | Function | `scripts/apply_refined_corrections.py`            | 140  |
| `has_sensor_window`                                              | Function | `scripts/apply_refined_corrections.py`            | 153  |
| `output_file_name`                                               | Function | `scripts/apply_refined_corrections.py`            | 162  |
| `save_corrected_files`                                           | Function | `scripts/apply_refined_corrections.py`            | 235  |
| `test_multiple_corrections_to_same_file_are_preserved`           | Function | `scripts/tests/test_apply_refined_corrections.py` | 78   |
| `test_save_corrected_files_escapes_malicious_cells`              | Function | `scripts/tests/test_apply_refined_corrections.py` | 189  |
| `test_determine_series_to_process_all_fallback`                  | Function | `scripts/tests/test_batch_correction.py`          | 656  |
| `test_determine_series_to_process_all_fallback_with_river_miles` | Function | `scripts/tests/test_batch_correction.py`          | 670  |
| `test_determine_series_to_process_explicit_invalid_value`        | Function | `scripts/tests/test_batch_correction.py`          | 697  |
| `test_determine_series_to_process_invalid_sensor_id_in_map`      | Function | `scripts/tests/test_batch_correction.py`          | 685  |
| `correct_gaps`                                                   | Function | `scripts/processor.py`                            | 278  |
| `test_generate_missing_times_hasattr_value`                      | Function | `scripts/tests/test_discontinuity_utils.py`       | 118  |
| `test_generate_missing_times_numeric`                            | Function | `scripts/tests/test_discontinuity_utils.py`       | 103  |
| `test_generate_missing_times_timestamp`                          | Function | `scripts/tests/test_discontinuity_utils.py`       | 89   |
| `test_load_raw_data_empty_file`                                  | Function | `scripts/tests/test_batch_correction.py`          | 640  |
| `find_matching_raw_file`                                         | Function | `scripts/export_comparison_sheets.py`             | 53   |
| `test_find_matching_raw_file_no_match`                           | Function | `scripts/tests/test_export_comparison_sheets.py`  | 59   |

## Execution Flows

| Flow                                                    | Type            | Steps |
| ------------------------------------------------------- | --------------- | ----- |
| `Main → _find_null_in_multiindex_level`                 | cross_community | 9     |
| `Main → _label_has_null_byte`                           | cross_community | 9     |
| `Main → _find_null_in_multiindex_level`                 | cross_community | 8     |
| `Main → _label_has_null_byte`                           | cross_community | 8     |
| `Save_corrected_files → _find_null_in_multiindex_level` | cross_community | 8     |
| `Save_corrected_files → _label_has_null_byte`           | cross_community | 8     |
| `Main → Escape_spreadsheet_formula`                     | cross_community | 7     |
| `Main → _is_sanitizable_dtype`                          | cross_community | 7     |
| `Save_corrected_files → Escape_spreadsheet_formula`     | cross_community | 7     |
| `Main → Escape_spreadsheet_formula`                     | cross_community | 7     |

## How to Explore

1. `context({name: "process_data"})` — see callers and callees
2. `query({search_query: "scripts"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings
   (source→sink data flows), when indexed with `--pdg`
