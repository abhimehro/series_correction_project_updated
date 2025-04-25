import os
import pandas as pd
from openpyxl import load_workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

# Set OUTPUT_DIR to the project root's output directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
SUMMARY_FILE = os.path.join(OUTPUT_DIR, "Summary_Report.xlsx")

def main():
    # Find all processed Excel files
    processed_files = sorted([
        f for f in os.listdir(OUTPUT_DIR)
        if f.endswith("_Processed.xlsx")
    ])
    if not processed_files:
        print(f"No processed files found in {OUTPUT_DIR}")
        return

    summary_data = []
    for file in processed_files:
        file_path = os.path.join(OUTPUT_DIR, file)
        try:
            df = pd.read_excel(file_path)
            mean_value = df['Processed_Value'].mean()
            median_value = df['Processed_Value'].median()
            outlier_count = df['Is_Outlier'].sum()
            summary_data.append({
                "File": file,
                "Mean_Processed_Value": mean_value,
                "Median_Processed_Value": median_value,
                "Outlier_Count": outlier_count
            })
        except Exception as e:
            print(f"Error processing {file}: {e}")

    summary_df = pd.DataFrame(summary_data)
    summary_df.to_excel(SUMMARY_FILE, index=False)

    # Format the summary Excel file
    wb = load_workbook(SUMMARY_FILE)
    ws = wb.active

    # Bold headers
    for col in range(1, ws.max_column + 1):
        ws.cell(row=1, column=col).font = Font(bold=True)

    # Adjust column widths
    for col in range(1, ws.max_column + 1):
        col_letter = get_column_letter(col)
        ws.column_dimensions[col_letter].width = 25

    # Create a bar chart for Outlier_Count
    chart = BarChart()
    chart.title = "Outlier Count per File"
    chart.x_axis.title = "File"
    chart.y_axis.title = "Outlier Count"

    data = Reference(ws, min_col=4, min_row=1, max_row=ws.max_row, max_col=4)  # Outlier_Count
    categories = Reference(ws, min_col=1, min_row=2, max_row=ws.max_row)       # File names
    chart.add_data(data, titles_from_data=True)
    chart.set_categories(categories)

    # Place the chart below the data
    ws.add_chart(chart, f"A{ws.max_row + 3}")

    wb.save(SUMMARY_FILE)
    print(f"Summary report with chart saved to: {SUMMARY_FILE}")

if __name__ == "__main__":
    main()