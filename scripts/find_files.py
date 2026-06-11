import glob
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# Directories to check
directories = [
    "data/output",
    "fixed_output",
    "output",
    "data",
    ".",  # Current directory
]

for directory in directories:
    if os.path.exists(directory):
        logger.info(f"\nChecking directory: {directory}")
        excel_files = glob.glob(os.path.join(directory, "**", "*.xlsx"), recursive=True)

        if excel_files:
            logger.info(f"Found {len(excel_files)} Excel files:")
            for file in excel_files:
                logger.info(f"  - {os.path.basename(file)}")
        else:
            logger.info("No Excel files found in this directory")
    else:
        logger.warning(f"\nDirectory does not exist: {directory}")

# Also check for other file types in the output directory
if os.path.exists("data/output"):
    logger.info("\nChecking for other file types in data/output:")
    all_files = glob.glob(os.path.join("data/output", "*.*"))
    if all_files:
        extensions = set(os.path.splitext(f)[1] for f in all_files)
        logger.info(f"File extensions found: {extensions}")
        for file in all_files:
            logger.info(f"  - {os.path.basename(file)}")
    else:
        logger.info("No files found")
