import os
import glob

# Directories to check
directories = [
    "data/output",
    "fixed_output",
    "output",
    "data",
    "."  # Current directory
]

for directory in directories:
    if os.path.exists(directory):
        print(f"\nChecking directory: {directory}")
        excel_files = glob.glob(os.path.join(directory, "**", "*.xlsx"), recursive=True)

        if excel_files:
            print(f"Found {len(excel_files)} Excel files:")
            for file in excel_files:
                print(f"  - {os.path.basename(file)}")
        else:
            print("No Excel files found in this directory")
    else:
        print(f"\nDirectory does not exist: {directory}")

# Also check for other file types in the output directory
if os.path.exists("data/output"):
    print("\nChecking for other file types in data/output:")
    all_files = glob.glob(os.path.join("data/output", "*.*"))
    if all_files:
        extensions = set(os.path.splitext(f)[1] for f in all_files)
        print(f"File extensions found: {extensions}")
        for file in all_files:
            print(f"  - {os.path.basename(file)}")
    else:
        print("No files found")