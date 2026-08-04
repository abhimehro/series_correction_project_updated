import pytest
from scripts.generate_overview_table import main
import sys

def trace_calls(frame, event, arg):
    if event == "call" and "generate_overview_table.py" in frame.f_code.co_filename:
        print(f"Calling: {frame.f_code.co_name}")
    elif event == "exception" and "generate_overview_table.py" in frame.f_code.co_filename:
        print(f"Exception in: {frame.f_code.co_name} -> {arg[0]}")
    return trace_calls

sys.settrace(trace_calls)
main("dummy_log.csv", "dummy_avg.csv")
sys.settrace(None)
