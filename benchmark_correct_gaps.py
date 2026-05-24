import time
import pandas as pd
import numpy as np
import logging
from scripts.processor import correct_gaps

logging.basicConfig(level=logging.WARNING)

def create_test_data(num_points=50000, num_gaps=2500):
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

df, gap_indices = create_test_data(50000, 2500)
print(f"DataFrame size: {len(df)}, Number of gaps: {len(gap_indices)}")

start_time = time.time()
result = correct_gaps(df, gap_indices)
end_time = time.time()

print(f"Original correct_gaps took: {end_time - start_time:.4f} seconds")
print(f"Result length: {len(result)}")
