# Guide to Correcting Year-to-Year Sensor Data Jumps

This guide explains the automated process used to identify and correct unexpected jumps in sensor data that occur
between the end of one year and the beginning of the next. Correcting these specific jumps helps ensure the data is
continuous and improves the accuracy of long-term trend analysis and visualizations for the Louisiana Freshwater Sponge
Project.

## 1. What's the Goal?

Sometimes, our sensor readings show a sudden, unrealistic shift when transitioning from the last data points recorded in
one year to the first data points of the following year. These "jumps" aren't real environmental changes; they can be
caused by things like sensor maintenance, calibration drift, or other factors.

These jumps make it hard to see the real long-term trends in how the riverbed is changing over many years and can make
our charts look messy and misleading.

Our goal is to use an automated process to find these specific jumps and make small, targeted adjustments (called "Level
Shifts") to the data in the later year. This adjusts the entire dataset for that sensor in that year up or down just
enough so that the readings line up smoothly with the end of the previous year. This way, our charts will show clearer,
more accurate long-term trends.

Here's an example of what a jump might look like in a chart before correction, and how correcting it helps smooth the
trend:

*(You can insert your "RM 54.0 Comparison (Series 27 Sensor 1 - Updated).jpg" here or a similar before/after
visualization)*

## 2. What You Need (The Ingredients)

To follow and run this automated process, you'll need:

* **Original Raw Sensor Data:** The original text files (`.txt`) containing the raw sensor readings for each series (
  like S26 and S27) and each year (e.g., `S26_Y01.txt`, `S27_Y20.txt`). These files are the untouched starting point.
* **Year-to-Year Differences Summary:** A summary file (specifically, the CSV exported from your analysis workbook, like
  `"Seatek_Analysis_Summary.xlsx - Year-to-Year Differences.csv"`) that lists the calculated differences between the end
  of one year and the start of the next for each sensor. This file acts as our instruction list, telling the automated
  process *where* the potentially problematic jumps are located (which year transition and which sensor).
* **The Correction Tools:** A set of Python scripts that contain the code to perform the automated steps. These scripts
  handle reading the data, calculating necessary adjustments, applying those adjustments, and generating summary files.
* **A Python Environment:** You need Python installed on your computer, along with necessary libraries like `pandas` for
  data handling. Using an environment like PyCharm (as you do) or setting up a virtual environment is recommended.
* **Excel Workbooks:** Your analysis workbooks where you create charts and reports.

## 3. How to Do It (The Automated Workflow Steps)

Here's the recipe for correcting the data jumps using the automated scripts:

* **Step 1: Identify the Jumps (Automated Detection)**
    * A Python script (let's call it `identify_outliers.py`) reads the "Year-to-Year Differences" summary CSV file.
    * It filters this data to find any year/sensor combinations where the absolute difference between the end of the
      previous year and the start of the next year is greater than or equal to our defined threshold (currently ±0.1).
    * This step effectively creates a list (in the script's memory) of all the specific locations in the data (which
      year pair and which sensor) that need a correction.

* **Step 2: Fix the Data (Automated Refined Level Shift Correction)**
    * Another Python script (let's call it `apply_refined_corrections.py`) uses the list of jumps identified in Step 1.
    * For each jump in the list, it loads the original raw data files for the two years involved (the year ending and
      the year starting).
    * It calculates the average of the last 5 valid (non-zero) readings from the previous year's raw data for the
      specific sensor, and the average of the first 5 valid (non-zero) readings from the next year's raw data for that
      same sensor.
    * It then calculates the precise "Level Shift" needed: this is the difference between the previous year's ending
      average and the next year's starting average. The goal is to add or subtract this amount from the next year's data
      to make its starting average match the previous year's ending average.
    * This calculated Level Shift is applied by adding it to *every single reading* for that sensor throughout the
      entire later year's raw data file. Data for other sensors and other years are not changed by this specific
      correction.
    * The script saves these adjusted data points into new CSV files in a dedicated output folder (e.g.,
      `corrected_output_refined_shift`). These new files will have `_refined_corrected.csv` added to their original
      filenames.

* **Step 3: Get the New Averages (Automated Summary Calculation)**
    * A third Python script (let's call it `calculate_updated_averages.py`) reads the *new, corrected* data files
      created in Step 2.
    * For every year and every sensor in these corrected files, it calculates the average of the first 5 valid (
      non-zero) readings and the average of the last 5 valid (non-zero) readings.
    * It saves these updated beginning and end averages, along with the recalculated year-to-year differences, into a
      new summary CSV file (like `"updated_beginning_end_averages.csv"`). This file contains the summary data based on
      the corrected raw data.

* **Step 4: Update Your Charts (Manual Update in Excel)**
    * Open your main Excel analysis workbook where your charts are located.
    * Go to the sheet where your original beginning and end averages are stored (the data that feeds your charts).
    * Import or copy the data from the `"updated_beginning_end_averages.csv"` file and paste it into this sheet,
      replacing the old averages.
    * Because your charts are linked to this data, they should automatically update to show the trends based on the
      corrected data's averages!

* **Step 5: Verify the Fix and Visualize (Visual Inspection)**
    * Look at your updated charts in Excel. Visually confirm that the year-to-year jumps you identified earlier have
      been smoothed out, and the overall trendlines look clearer and more continuous. You can also refer to the "
      Effectiveness Evaluation" data/sheet to see the numerical confirmation that the differences are now near zero.

* **Step 6: Record the Changes (Audit Trail)**
    * Refer to the correction log file (`correction_log_refined_shift.csv`). This file was automatically created in Step
      2 and lists every single correction that was applied, including which file and sensor were changed and by how
      much (the calculated level shift). This is your essential record of exactly what adjustments were made, providing
      transparency for your analysis.

## 4. Why This Refined Approach Works (Simple Explanation)

This method is powerful because it doesn't rely on a single, potentially inaccurate pre-calculated difference. Instead,
for each specific jump identified, it goes back to the raw data right at that transition point. By calculating the
difference between the average of the last few valid readings of the previous year and the average of the first few
valid readings of the next year, it figures out the *exact* shift needed to make those two points meet smoothly.
Applying this precise shift to the entire next year's data for that sensor corrects the baseline without distorting the
trends within that year. It's a targeted, data-driven fix for each individual jump.

## 5. Keeping Track (Important Notes)

* **Always keep your original raw data files safe and untouched.** The automated correction process creates *new*
  corrected files; it does not modify your original data.
* **Store the correction log file (`correction_log_refined_shift.csv`) with your project documentation.** It's your
  complete record of all applied changes and is vital for transparency and reproducibility.
* **Store the `updated_beginning_end_averages.csv` file.** This is your source for the summary data based on the
  corrected raw data.
* If you get new raw data in the future, you can run these same Python scripts on the new data to apply consistent
  corrections, making your workflow dynamic.

This process makes correcting the data more automated, consistent, transparent, and easier to understand, helping you
focus on the important environmental insights from your sensor data for the Louisiana Freshwater Sponge Project!