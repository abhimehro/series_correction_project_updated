import pandas as pd
import numpy as np

df = pd.DataFrame({'Time (Seconds)': [1, 2, 8, 9, 15]})
gap_indices_np = np.array([2, 4])

# Standard way
print("Standard list:", gap_indices_np.tolist())

# Indexing way
print("Index list:", df.index[gap_indices_np].tolist())

# Custom index
df2 = pd.DataFrame({'Time (Seconds)': [1, 2, 8, 9, 15]}, index=[10, 20, 30, 40, 50])
print("Custom index list:", df2.index[gap_indices_np].tolist())
