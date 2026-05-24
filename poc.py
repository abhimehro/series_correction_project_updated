import pandas as pd
import numpy as np
import time

def create_test_data(num_points=10000, num_gaps=500):
    # Create base data
    times = np.arange(num_points, dtype=float)
    values = np.sin(times / 10.0)

    df = pd.DataFrame({
        "Time (Seconds)": times,
        "Value": values
    })

    # Introduce gaps by dropping rows
    np.random.seed(42)
    gap_starts = np.random.choice(np.arange(10, num_points - 10), size=num_gaps, replace=False)

    # Drop rows to create gaps of size 2-5
    drop_indices = []
    for start in gap_starts:
        gap_size = np.random.randint(2, 6)
        drop_indices.extend(range(start, start + gap_size))

    df = df.drop(index=drop_indices).reset_index(drop=True)

    # Find gap indices as detect_gaps would
    time_diffs = df["Time (Seconds)"].diff()
    median_diff = time_diffs.median()

    # Gaps where diff > 1.5 * median
    gap_indices = df[time_diffs > 1.5 * median_diff].index.tolist()

    return df, gap_indices

def correct_gaps_old(
    data: pd.DataFrame,
    gap_indices: list[int],
    time_col: str = "Time (Seconds)",
) -> pd.DataFrame:
    result_df = data.copy()
    result_df = result_df.sort_values(by=time_col).reset_index(drop=True)
    processed_gap_indices = set()

    for gap_idx in sorted(gap_indices, reverse=True):
        if gap_idx in processed_gap_indices or gap_idx == 0:
            continue

        idx_before = gap_idx - 1
        idx_after = gap_idx

        time_before = result_df[time_col].iloc[idx_before]
        time_after = result_df[time_col].iloc[idx_after]

        if idx_before > 0:
            time_prev = result_df[time_col].iloc[idx_before - 1]
            normal_step = time_before - time_prev
        elif len(result_df) > idx_after + 1:
            time_next = result_df[time_col].iloc[idx_after + 1]
            normal_step = time_next - time_after
        else:
            continue

        if normal_step <= 0:
            continue

        num_missing_points = round((time_after - time_before) / normal_step) - 1

        if num_missing_points <= 0:
            continue

        new_times = np.linspace(
            time_before + normal_step,
            time_after - normal_step,
            num=num_missing_points,
            dtype=type(time_before),
        )

        new_rows_list = []
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

    return result_df

def correct_gaps_new(
    data: pd.DataFrame,
    gap_indices: list[int],
    time_col: str = "Time (Seconds)",
) -> pd.DataFrame:
    result_df = data.copy()
    result_df = result_df.sort_values(by=time_col).reset_index(drop=True)
    processed_gap_indices = set()

    all_new_rows = []

    for gap_idx in sorted(gap_indices, reverse=True):
        if gap_idx in processed_gap_indices or gap_idx == 0:
            continue

        idx_before = gap_idx - 1
        idx_after = gap_idx

        time_before = result_df[time_col].iloc[idx_before]
        time_after = result_df[time_col].iloc[idx_after]

        if idx_before > 0:
            time_prev = result_df[time_col].iloc[idx_before - 1]
            normal_step = time_before - time_prev
        elif len(result_df) > idx_after + 1:
            time_next = result_df[time_col].iloc[idx_after + 1]
            normal_step = time_next - time_after
        else:
            continue

        if normal_step <= 0:
            continue

        num_missing_points = round((time_after - time_before) / normal_step) - 1

        if num_missing_points <= 0:
            continue

        new_times = np.linspace(
            time_before + normal_step,
            time_after - normal_step,
            num=num_missing_points,
            dtype=type(time_before),
        )

        for t in new_times:
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

    return result_df

df, gap_indices = create_test_data(50000, 2500)

print(f"DataFrame size: {len(df)}, Number of gaps: {len(gap_indices)}")

start_time = time.time()
old_res = correct_gaps_old(df, gap_indices)
end_time = time.time()
print(f"Old took: {end_time - start_time:.4f} seconds")

start_time = time.time()
new_res = correct_gaps_new(df, gap_indices)
end_time = time.time()
print(f"New took: {end_time - start_time:.4f} seconds")

print("Match?", old_res.equals(new_res))
