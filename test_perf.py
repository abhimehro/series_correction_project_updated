import pandas as pd
import numpy as np

def create_test_data(num_points=100, num_gaps=10):
    times = np.arange(num_points, dtype=float)
    values = np.sin(times / 10.0)

    df = pd.DataFrame({
        "Time (Seconds)": times,
        "Value": values
    })

    np.random.seed(42)
    gap_starts = np.random.choice(np.arange(10, num_points - 10), size=num_gaps, replace=False)

    drop_indices = []
    for start in gap_starts:
        gap_size = np.random.randint(2, 6)
        drop_indices.extend(range(start, start + gap_size))

    df = df.drop(index=drop_indices).reset_index(drop=True)
    time_diffs = df["Time (Seconds)"].diff()
    median_diff = time_diffs.median()
    gap_indices = df[time_diffs > 1.5 * median_diff].index.tolist()
    return df, gap_indices

from scripts.processor import correct_gaps
df, gi = create_test_data(100, 10)
res = correct_gaps(df, gi)
print("Result Length:", len(res))
