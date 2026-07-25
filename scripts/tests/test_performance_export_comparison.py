import time
import numpy as np
import pandas as pd


def test_performance():
    # create dummy data
    N = 100000
    df = pd.DataFrame({"Outlier_Flag": [False] * N})
    outlier_indices = np.random.choice(N, size=int(N * 0.05), replace=False).tolist()

    # Old code
    df_old = df.copy()
    start_time = time.time()
    for idx in outlier_indices:
        if idx < len(df_old):
            df_old.at[idx, "Outlier_Flag"] = True
    old_time = time.time() - start_time
    print(f"Old time: {old_time:.6f}s")

    # New code
    df_new = df.copy()
    start_time = time.time()
    df_new.loc[df_new.index.isin(outlier_indices), "Outlier_Flag"] = True
    new_time = time.time() - start_time
    print(f"New time: {new_time:.6f}s")

    assert df_old["Outlier_Flag"].equals(df_new["Outlier_Flag"])
    print(f"Improvement: {old_time / new_time:.2f}x faster")


if __name__ == "__main__":
    test_performance()
