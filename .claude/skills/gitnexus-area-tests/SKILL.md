---
name: gitnexus-area-tests
description: "Skill for the Tests area of series_correction_project_updated. 165 symbols across 27 files."
---

# Tests

165 symbols | 27 files | Cohesion: 90%

## When to Use

- Working with code in `scripts/`
- Understanding how batch_process, test_batch_process_config_not_found,
  test_batch_process_data_dir_not_found work
- Modifying tests-related functionality

## Key Files

| File                                              | Symbols                                                                                                                                                                                                                                                           |
| ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/tests/test_apply_refined_corrections.py` | test_calculate_non_zero_average_all_nans, test_calculate_non_zero_average_all_zeros, test_calculate_non_zero_average_basic, test_calculate_non_zero_average_booleans, test_calculate_non_zero_average_complex_objects (+19)                                       |
| `scripts/tests/test_processor.py`                 | test_detect_outliers_basic, test_detect_outliers_no_outliers, test_detect_outliers_small_data, test_detect_outliers_with_nans, test_detect_outliers_zero_mad (+10)                                                                                                |
| `scripts/tests/test_batch_correction.py`          | test_batch_process_config_not_found, test_batch_process_data_dir_not_found, test_batch_process_dry_run, test_batch_process_invalid_series_selection, test_batch_process_load_error (+9)                                                                           |
| `scripts/tests/test_spreadsheet_safety.py`        | test_sanitize_dataframe_categorical_collision_fallback, test_sanitize_dataframe_does_not_mutate_input, test_sanitize_dataframe_escapes_category_columns, test_sanitize_dataframe_escapes_column_and_index_labels, test_sanitize_dataframe_escapes_multiindex (+8) |
| `scripts/tests/test_discontinuity_utils.py`       | test_validate_gap_parameters_invalid_step, test_validate_gap_parameters_negative_missing_points, test_validate_gap_parameters_normal_step_none, test_validate_gap_parameters_valid, test_validate_gap_parameters_zero_missing_points (+6)                         |
| `scripts/batch_correction.py`                     | _determine_year_for_index, _enrich_config_with_river_mappings, _ensure_output_directory, _find_files_to_process, _get_data_directory (+5)                                                                                                                         |
| `scripts/processor.py`                            | _calculate_outlier_indices, _validate_outlier_inputs, detect_outliers, _calculate_median_time_diff, _find_gap_indices (+4)                                                                                                                                        |
| `scripts/tests/test_export_sink_guard.py`         | _find_python_files, _format_violation, _is_excluded, _scan_file, test_no_unauthorized_spreadsheet_sinks (+4)                                                                                                                                                      |
| `scripts/apply_refined_corrections.py`            | calculate_non_zero_average, find_sensor_columns, load_identified_outliers, parse_year_pair, parse_sensor_index                                                                                                                                                    |
| `scripts/tests/test_series_correction_cli.py`     | test_main_batch_process_exception, test_main_batch_process_value_error, test_main_happy_path, test_main_missing_required_args, test_main_with_dry_run_flag                                                                                                        |

## Entry Points

Start here when exploring this area:

- **`batch_process`** (Function) — `scripts/batch_correction.py:460`
- **`test_batch_process_config_not_found`** (Function) —
  `scripts/tests/test_batch_correction.py:614`
- **`test_batch_process_data_dir_not_found`** (Function) —
  `scripts/tests/test_batch_correction.py:345`
- **`test_batch_process_dry_run`** (Function) —
  `scripts/tests/test_batch_correction.py:279`
- **`test_batch_process_invalid_series_selection`** (Function) —
  `scripts/tests/test_batch_correction.py:518`

## Key Symbols

| Symbol                                                 | Type     | File                                              | Line |
| ------------------------------------------------------ | -------- | ------------------------------------------------- | ---- |
| `BatchConfig`                                          | Class    | `scripts/batch_correction.py`                     | 451  |
| `batch_process`                                        | Function | `scripts/batch_correction.py`                     | 460  |
| `test_batch_process_config_not_found`                  | Function | `scripts/tests/test_batch_correction.py`          | 614  |
| `test_batch_process_data_dir_not_found`                | Function | `scripts/tests/test_batch_correction.py`          | 345  |
| `test_batch_process_dry_run`                           | Function | `scripts/tests/test_batch_correction.py`          | 279  |
| `test_batch_process_invalid_series_selection`          | Function | `scripts/tests/test_batch_correction.py`          | 518  |
| `test_batch_process_load_error`                        | Function | `scripts/tests/test_batch_correction.py`          | 461  |
| `test_batch_process_no_files_found`                    | Function | `scripts/tests/test_batch_correction.py`          | 323  |
| `test_batch_process_process_error`                     | Function | `scripts/tests/test_batch_correction.py`          | 492  |
| `test_batch_process_skip_empty_file`                   | Function | `scripts/tests/test_batch_correction.py`          | 402  |
| `test_batch_process_with_processor_module`             | Function | `scripts/tests/test_batch_correction.py`          | 438  |
| `test_get_data_directory_creates_dir`                  | Function | `scripts/tests/test_batch_correction.py`          | 375  |
| `test_get_data_directory_creates_dir_oserror`          | Function | `scripts/tests/test_batch_correction.py`          | 392  |
| `test_batch_process_routes_through_write_excel_safely` | Function | `scripts/tests/test_batch_correction_routing.py`  | 9    |
| `calculate_non_zero_average`                           | Function | `scripts/apply_refined_corrections.py`            | 20   |
| `test_calculate_non_zero_average_all_nans`             | Function | `scripts/tests/test_apply_refined_corrections.py` | 54   |
| `test_calculate_non_zero_average_all_zeros`            | Function | `scripts/tests/test_apply_refined_corrections.py` | 42   |
| `test_calculate_non_zero_average_basic`                | Function | `scripts/tests/test_apply_refined_corrections.py` | 18   |
| `test_calculate_non_zero_average_booleans`             | Function | `scripts/tests/test_apply_refined_corrections.py` | 164  |
| `test_calculate_non_zero_average_complex_objects`      | Function | `scripts/tests/test_apply_refined_corrections.py` | 183  |

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

1. `context({name: "batch_process"})` — see callers and callees
2. `query({search_query: "tests"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings
   (source→sink data flows), when indexed with `--pdg`
