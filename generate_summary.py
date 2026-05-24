import os

import pandas as pd
from openpyxl import load_workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

from scripts.spreadsheet_safety import write_excel_safely

# Set OUTPUT_DIR to the project root's output directory
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
SUMMARY_FILE = os.path.join(OUTPUT_DIR, "Summary_Report.xlsx")


def get_processed_files(output_dir):
    return sorted([f for f in os.listdir(output_dir) if f.endswith("_Processed.xlsx")])


def aggregate_summary_data(processed_files, output_dir):
    summary_data = []
    for file in processed_files:
        file_path = os.path.join(output_dir, file)
        try:
            df = pd.read_excel(file_path)
            mean_value = df["Processed_Value"].mean()
            median_value = df["Processed_Value"].median()
            outlier_count = df["Is_Outlier"].sum()
            summary_data.append(
                {
                    "File": file,
                    "Mean_Processed_Value": mean_value,
                    "Median_Processed_Value": median_value,
                    "Outlier_Count": outlier_count,
                }
            )
        except Exception as e:
            print(f"Error processing {file}: {e}")
    return pd.DataFrame(summary_data)


def format_and_add_chart(excel_file):
    wb = load_workbook(excel_file)
    ws = wb.active

    for col in range(1, ws.max_column + 1):
        ws.cell(row=1, column=col).font = Font(bold=True)

    for col in range(1, ws.max_column + 1):
        col_letter = get_column_letter(col)
        ws.column_dimensions[col_letter].width = 25

    chart = BarChart()
    chart.title = "Outlier Count per File"
    chart.x_axis.title = "File"
    chart.y_axis.title = "Outlier Count"

    data = Reference(ws, min_col=4, min_row=1, max_row=ws.max_row, max_col=4)
    categories = Reference(ws, min_col=1, min_row=2, max_row=ws.max_row)
    chart.add_data(data, titles_from_data=True)
    chart.set_categories(categories)

    ws.add_chart(chart, f"A{ws.max_row + 3}")
    wb.save(excel_file)


def main():
    processed_files = get_processed_files(OUTPUT_DIR)
    if not processed_files:
        print(f"No processed files found in {OUTPUT_DIR}")
        return

    summary_df = aggregate_summary_data(processed_files, OUTPUT_DIR)
    write_excel_safely(summary_df, SUMMARY_FILE, index=False)
    format_and_add_chart(SUMMARY_FILE)
    print(f"Summary report with chart saved to: {SUMMARY_FILE}")


if __name__ == "__main__":
    main()
