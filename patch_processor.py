import re

with open("scripts/processor.py", "r") as f:
    content = f.read()

# We need to replace the part inside correct_gaps starting around line 312
# The old code:
#     result_df = result_df.sort_values(by=time_col).reset_index(drop=True)
#     processed_gap_indices = set()
#
#     for gap_idx in sorted(gap_indices, reverse=True):
#         if gap_idx in processed_gap_indices or gap_idx == 0:
#             continue
# ...
#         new_rows_list = []
#         for t in new_times:
#             new_row = {time_col: t}
#             for col in result_df.columns:
#                 if col != time_col:
#                     new_row[col] = np.nan
#             new_rows_list.append(new_row)
#
#         if not new_rows_list:
#             continue
#
#         new_rows_df = pd.DataFrame(new_rows_list)
#
#         result_df = pd.concat(
#             [result_df.iloc[:gap_idx], new_rows_df, result_df.iloc[gap_idx:]]
#         ).reset_index(drop=True)
#
#         processed_gap_indices.add(gap_idx)
#
#     log.info(


old_str = """    result_df = result_df.sort_values(by=time_col).reset_index(drop=True)
    processed_gap_indices = set()

    for gap_idx in sorted(gap_indices, reverse=True):"""

new_str = """    result_df = result_df.sort_values(by=time_col).reset_index(drop=True)
    processed_gap_indices = set()
    all_new_rows = []

    for gap_idx in sorted(gap_indices, reverse=True):"""

content = content.replace(old_str, new_str)

old_str2 = """        new_rows_list = []
        for t in new_times:
            new_row = {time_col: t}
            for col in result_df.columns:
                if col != time_col:
                    new_row[col] = np.nan
            new_rows_list.append(new_row)

        if not new_rows_list:
            continue

        new_rows_df = pd.DataFrame(new_rows_list)

        result_df = pd.concat(
            [result_df.iloc[:gap_idx], new_rows_df, result_df.iloc[gap_idx:]]
        ).reset_index(drop=True)

        processed_gap_indices.add(gap_idx)

    log.info("""

new_str2 = """        for t in new_times:
            new_row = {time_col: t}
            for col in result_df.columns:
                if col != time_col:
                    new_row[col] = np.nan
            all_new_rows.append(new_row)

        processed_gap_indices.add(gap_idx)

    if all_new_rows:
        new_rows_df = pd.DataFrame(all_new_rows)
        result_df = pd.concat([result_df, new_rows_df], ignore_index=True)
        result_df = result_df.sort_values(by=time_col).reset_index(drop=True)

    log.info("""

content = content.replace(old_str2, new_str2)

with open("scripts/processor.py", "w") as f:
    f.write(content)
