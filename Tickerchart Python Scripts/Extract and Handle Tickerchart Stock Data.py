# -*- coding: utf-8 -*-

import pandas as pd
from datetime import date
import os

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

BASE_PATH = r"C:\AhmedElsayed-Reliable Work Space\TickerChart_Data"
INPUT_FILE = "Ticker_Chart - Copy.csv"

OUTPUT_PATHS = [
    r"C:\AhmedElsayed-Reliable Work Space\TickerChart_Data\TickerChart orignal data"
]

TODAY_DATE = date.today().strftime('%Y-%m-%d')

ENCODING = "iso-8859-1"

# Columns used in the dataset
SELECTED_COLUMNS = [
    'Symbol', 'Volume', 'Value', 'Trades', 'Last', 'Open',
    'NetFlow(by Val) (day)', 'Inflow Percentage %',
    'InFlow(by Val) (day)', 'OutFlow(by Val) (day)',
    'Prev. Close', 'Money Flow', 'Last', 'Change',
    'NetFlow(by Volme) (day)', 'INFlow(by Volume) (day)',
    'OutFlow(by Volume) (day)', 'Close'
]

FLOW_COLUMNS = [
    'NetFlow(by Val) (day)',
    'InFlow(by Val) (day)',
    'OutFlow(by Val) (day)',
    'NetFlow(by Volme) (day)',
    'INFlow(by Volume) (day)',
    'OutFlow(by Volume) (day)'
]

FINAL_COLUMNS_ORDER = [
    'Date', 'Symbol', 'Volume', 'Value', 'Trades', 'Last', 'Open',
    'NetFlow(by Val) (day)', 'Inflow Percentage %',
    'InFlow(by Val) (day)', 'OutFlow(by Val) (day)',
    'Prev. Close', 'Money Flow', 'Last', 'Change',
    'NetFlow(by Volme) (day)', 'INFlow(by Volume) (day)',
    'OutFlow(by Volume) (day)', 'Close'
]

# ------------------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------------------

def clean_flow_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Replace '-' with 0, remove commas, and cast columns to float.
    """
    for col in columns:
        df[col] = (
            df[col]
            .apply(lambda x: 0 if x == "-" else x)
            .astype(str)
            .apply(lambda x: x.replace(",", ""))
            .astype(float)
        )
    return df


# ------------------------------------------------------------------------------
# Main Processing
# ------------------------------------------------------------------------------

# Read input file
input_file_path = os.path.join(BASE_PATH, INPUT_FILE)
df = pd.read_csv(input_file_path, encoding=ENCODING)

# Select required columns
df = df[SELECTED_COLUMNS]

# Clean flow-related columns
df = clean_flow_columns(df, FLOW_COLUMNS)

# Update symbol and add date
df["Symbol"] = df["Symbol"] + " FLOW "
df["Date"] = TODAY_DATE

# Reorder columns
df = df[FINAL_COLUMNS_ORDER]

# ------------------------------------------------------------------------------
# Export Results
# ------------------------------------------------------------------------------

for output_path in OUTPUT_PATHS:
    output_file = f"Ticker_Chart({TODAY_DATE}).csv"
    full_output_path = os.path.join(output_path, output_file)

    df.to_csv(full_output_path, index=False)

    print(f"TickerChart data extracted on ({TODAY_DATE})")
    print(f"Saved successfully at: {output_path}")
    print("-" * 70)
