# -*- coding: utf-8 -*-

import cx_Oracle
import pandas as pd
import csv
import os
from datetime import date

# ==============================================================================
# Configuration
# ==============================================================================

BASE_PATH = r"C:/AhmedElsayed-Reliable Work Space/TickerChart_Data"
RAW_FILE = "Ticker_Chart - Copy.csv"
ENCODING = "iso-8859-1"

TODAY_DATE = date.today().strftime('%Y-%m-%d')

ORIGINAL_DATA_PATH = rf"{BASE_PATH}/TickerChart orignal data"
NETFLOW_PATH = rf"{BASE_PATH}/Tickers_(NETFLOW)"
NETFLOW_STANDALONE_PATH = rf"{BASE_PATH}/Tickers_(NETFLOW)Stand alone"

# ==============================================================================
# Common Columns
# ==============================================================================

FLOW_COLUMNS = [
    "NetFlow(by Val) (day)", "InFlow(by Val) (day)", "OutFlow(by Val) (day)",
    "NetFlow(by Volme) (day)", "INFlow(by Volume) (day)", "OutFlow(by Volume) (day)"
]

BASE_COLUMNS = [
    'Symbol', 'Volume', 'Value', 'Trades', 'Close', 'Last', 'Open',
    *FLOW_COLUMNS, 'Inflow Percentage %', 'Prev. Close', 'Money Flow', 'Change'
]

# ==============================================================================
# Helper Functions
# ==============================================================================

def clean_numeric_column(series):
    return (
        series.apply(lambda x: 0 if x == "-" else x)
              .astype(str)
              .apply(lambda x: x.replace(",", ""))
              .astype(float)
    )


def clean_flow_dataframe(df, columns):
    for col in columns:
        df[col] = clean_numeric_column(df[col])
    return df


def db_connect():
    con = cx_Oracle.connect(
        'STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK'
    )
    return con


# ==============================================================================
# 1️⃣ Process 1 – Clean Raw TickerChart Data
# ==============================================================================

raw_df = pd.read_csv(
    rf"{BASE_PATH}/{RAW_FILE}",
    encoding=ENCODING
)

raw_df = raw_df[
    ['Symbol', 'Volume', 'Value', 'Trades', 'Last', 'Open',
     *FLOW_COLUMNS, 'Inflow Percentage %', 'Prev. Close',
     'Money Flow', 'Last', 'Change', 'Close']
]

raw_df = clean_flow_dataframe(raw_df, FLOW_COLUMNS)

raw_df["Symbol"] = raw_df["Symbol"] + " FLOW "
raw_df["Date"] = TODAY_DATE

raw_df = raw_df[
    ['Date', 'Symbol', 'Volume', 'Value', 'Trades', 'Last', 'Open',
     *FLOW_COLUMNS, 'Inflow Percentage %', 'Prev. Close',
     'Money Flow', 'Last', 'Change', 'Close']
]

os.makedirs(ORIGINAL_DATA_PATH, exist_ok=True)
raw_df.to_csv(
    rf"{ORIGINAL_DATA_PATH}/Ticker_Chart({TODAY_DATE}).csv",
    index=False
)

print("1st Process done – Cleaned TickerChart data generated")

# ==============================================================================
# 2️⃣ Process 2 – Generate NetFlow CSVs (Per Ticker)
# ==============================================================================

df = pd.read_csv(
    rf"{ORIGINAL_DATA_PATH}/Ticker_Chart({TODAY_DATE}).csv"
)

df = df[['Symbol', *FLOW_COLUMNS]]
df["Date"] = TODAY_DATE

os.makedirs(NETFLOW_STANDALONE_PATH, exist_ok=True)

for _, row in df.iterrows():

    if (
        row["InFlow(by Val) (day)"] == 0 and
        row["OutFlow(by Val) (day)"] == 0 and
        row["INFlow(by Volume) (day)"] == 0 and
        row["OutFlow(by Volume) (day)"] == 0
    ):
        continue

    file_path = rf"{NETFLOW_STANDALONE_PATH}/{row['Symbol']}(Daily NETFLOW).csv"

    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "DATE", "TICKER",
                "NET(Value)FLOW", "IN(Value)FLOW", "OUT(Value)FLOW",
                "NET(Volume)FLOW", "IN(Volume)FLOW", "OUT(Volume)FLOW"
            ]
        )

        writer.writerow({
            "DATE": row["Date"],
            "TICKER": row["Symbol"],
            "NET(Value)FLOW": row["NetFlow(by Val) (day)"],
            "IN(Value)FLOW": row["InFlow(by Val) (day)"],
            "OUT(Value)FLOW": row["OutFlow(by Val) (day)"],
            "NET(Volume)FLOW": row["NetFlow(by Volme) (day)"],
            "IN(Volume)FLOW": row["INFlow(by Volume) (day)"],
            "OUT(Volume)FLOW": row["OutFlow(by Volume) (day)"]
        })

print("2nd Process done – NetFlow CSVs generated")

# ==============================================================================
# 3️⃣ Process 3 – Insert Latest NetFlow into Oracle DB
# ==============================================================================

con = db_connect()
cursor = con.cursor()

files = [
    f for f in os.listdir(NETFLOW_STANDALONE_PATH)
    if f.endswith(".csv")
]

for file in files:
    df = pd.read_csv(rf"{NETFLOW_STANDALONE_PATH}/{file}")
    df["DATE"] = pd.to_datetime(df["DATE"], utc=True)
    df.set_index("DATE", inplace=True)
    df.sort_index(inplace=True)

    last_row = df.tail(1)

    for idx, row in last_row.iterrows():
        try:
            cursor.execute(
                """
                INSERT INTO FILL_OHLCV
                (TICKER, OPEN, HIGH, LOW, CLOSE, VOLUME, BARTIMESTAMP, ASSET, VWAP)
                VALUES
                (:1, :2, :3, :4, :5, :6, :7, :8, :9)
                """,
                [
                    row["TICKER"],
                    row["NET(Value)FLOW"],
                    row["IN(Value)FLOW"],
                    row["OUT(Value)FLOW"],
                    row["NET(Volume)FLOW"],
                    row["IN(Volume)FLOW"],
                    idx.to_pydatetime(),
                    1,
                    row["OUT(Volume)FLOW"]
                ]
            )
            con.commit()
            print(f"{row['TICKER']} inserted successfully")

        except Exception as e:
            print(f"Insert failed for {row['TICKER']}")
            print(e)

print("3rd Process done – Database insertion completed")
