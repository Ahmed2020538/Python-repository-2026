import os
import csv
from datetime import date
import pandas as pd
import cx_Oracle

# =============================================================================
# Configuration
# =============================================================================
BASE_PATH = "C:/AhmedElsayed-Reliable Work Space/TickerChart_Data"
RAW_DATA_PATH = f"{BASE_PATH}/TickerChart orignal data"
NETFLOW_PATH = f"{BASE_PATH}/Tickers_(NETFLOW)"
STANDALONE_PATH = f"{BASE_PATH}/Tickers_(NETFLOW)Stand alone"

TODAY = date.today().strftime("%Y-%m-%d")

RAW_FILE = f"{RAW_DATA_PATH}/Ticker_Chart({TODAY}).csv"

FLOW_COLUMNS = [
    'Symbol', 'Volume', 'Value', 'Trades', 'Close', 'Last', 'Open',
    'NetFlow(by Val) (day)', 'Inflow Percentage %',
    'InFlow(by Val) (day)', 'OutFlow(by Val) (day)', 'Prev. Close',
    'Money Flow', 'Last', 'Change',
    'NetFlow(by Volme) (day)', 'INFlow(by Volume) (day)',
    'OutFlow(by Volume) (day)'
]

NETFLOW_FIELDS = [
    "DATE", "TICKER",
    "NET(Value)FLOW", "IN(Value)FLOW", "OUT(Value)FLOW",
    "NET(Volume)FLOW", "IN(Volume)FLOW", "OUT(Volume)FLOW"
]

# =============================================================================
# Utility Functions
# =============================================================================
def clean_numeric_column(series: pd.Series) -> pd.Series:
    """
    Replace '-' with 0, remove commas, and cast to float.
    """
    return (
        series
        .replace("-", 0)
        .astype(str)
        .str.replace(",", "")
        .astype(float)
    )


def load_and_prepare_raw_data(add_flow_suffix: bool = False) -> pd.DataFrame:
    """
    Load raw TickerChart CSV and prepare base dataframe.
    """
    df = pd.read_csv(RAW_FILE)
    df = df[FLOW_COLUMNS].copy()

    df["Date"] = TODAY
    df["Symbol"] = df["Symbol"].astype(str)

    if add_flow_suffix:
        df["Symbol"] = df["Symbol"] + " FLOW "
    else:
        df["Symbol"] = df["Symbol"].apply(lambda x: x.split(" ")[0])

    return df


# =============================================================================
# Stage 1 – Extract Full NetFlow CSV
# =============================================================================
def export_full_netflow_file():
    df = load_and_prepare_raw_data(add_flow_suffix=True)

    ordered_cols = ["Date"] + [col for col in FLOW_COLUMNS]
    df = df[ordered_cols]
    df = df.set_index("Date")

    output_file = f"{NETFLOW_PATH}/Tickers_NETFLOWs_TickerChart({TODAY}).csv"
    df.to_csv(output_file)

    print(f"[INFO] Full NetFlow file exported for {TODAY}")


# =============================================================================
# Stage 2 – Per-Ticker Standalone NetFlow Files
# =============================================================================
def export_ticker_standalone_files():
    df = load_and_prepare_raw_data(add_flow_suffix=False)

    for _, row in df.iterrows():
        in_val  = row["InFlow(by Val) (day)"]
        out_val = row["OutFlow(by Val) (day)"]
        in_vol  = row["INFlow(by Volume) (day)"]
        out_vol = row["OutFlow(by Volume) (day)"]

        if in_val == 0 and out_val == 0 and in_vol == 0 and out_vol == 0:
            print(f"[SKIP] {row['Symbol']} has zero IN/OUT flows")
            continue

        file_path = f"{STANDALONE_PATH}/{row['Symbol']}(Daily NETFLOW).csv"

        with open(file_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=NETFLOW_FIELDS)
            writer.writerow({
                "DATE": TODAY,
                "TICKER": row["Symbol"],
                "NET(Value)FLOW": row["NetFlow(by Val) (day)"],
                "IN(Value)FLOW": in_val,
                "OUT(Value)FLOW": out_val,
                "NET(Volume)FLOW": row["NetFlow(by Volme) (day)"],
                "IN(Volume)FLOW": in_vol,
                "OUT(Volume)FLOW": out_vol
            })

    print("[INFO] Standalone ticker NetFlow files updated")


# =============================================================================
# Stage 3 – Index (EGX) Aggregation
# =============================================================================
def fetch_index_symbols(sql: str, connection) -> list:
    df = pd.read_sql(sql, connection)
    return df["REUTERS"].apply(lambda x: x.split(".")[0]).unique()


def export_index_netflows():
    con = cx_Oracle.connect("STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK")

    EGX50 = [
        "ABUK","EMFD","ALCN","AMOC","COMI","SWDY","EAST","EKHO","EKHOA","ETEL",
        "ORWE","ORAS","EFIH","EFID","PHDC","BTFH","FAITA","FAIT","CIEB","BINV",
        "JUFO","GBCO","ESRS","SKPC","CLHO","FWRY","HRHO","TMGH","MASR","HELI",
        "MFPC","ADIB","CCAP","CIRA","EFIC","EGAL","EGCH","HDBK","ISPH","MCQE",
        "OIH","OLFI","PHAR","RMDA","SAUD","TALM","ZMID","ASCM","OCDI","ARCC"
    ]

    EGX70 = fetch_index_symbols("""
        SELECT SYMBOLINFO.REUTERS
        FROM EGX70_SYMBOLS
        LEFT JOIN SYMBOLINFO ON EGX70_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE
    """, con)

    EGX100 = fetch_index_symbols("""
        SELECT SYMBOLINFO.REUTERS
        FROM EGX100_SYMBOLS
        LEFT JOIN SYMBOLINFO ON EGX100_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE
    """, con)

    SHARIAH = fetch_index_symbols("""
        SELECT SYMBOLINFO.REUTERS
        FROM EGX_SHARIAH_SYMBOLS
        LEFT JOIN SYMBOLINFO ON EGX_SHARIAH_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE
    """, con)

    index_configs = [
        ("EGX50EWI FLOW ", "EGX50LASTEWI FLOW ", EGX50),
        ("EGX70EWI FLOW ", "EGX70 FLOW ", EGX70),
        ("EGX100EWI FLOW ", "EGX100 FLOW ", EGX100),
        ("SHARIAH FLOW ", "SHARIA FLOW ", SHARIAH),
    ]

    df = load_and_prepare_raw_data(add_flow_suffix=False)

    for col in [
        "NetFlow(by Val) (day)", "InFlow(by Val) (day)", "OutFlow(by Val) (day)",
        "NetFlow(by Volme) (day)", "INFlow(by Volume) (day)", "OutFlow(by Volume) (day)",
        "Volume", "Value", "Close", "Open"
    ]:
        df[col] = clean_numeric_column(df[col])

    for display_name, file_ticker, symbols in index_configs:
        subset = df[df["Symbol"].isin(symbols)]

        if subset.empty:
            continue

        values = {
            "NET(Value)FLOW": subset["NetFlow(by Val) (day)"].sum(),
            "IN(Value)FLOW": subset["InFlow(by Val) (day)"].sum(),
            "OUT(Value)FLOW": subset["OutFlow(by Val) (day)"].sum(),
            "NET(Volume)FLOW": subset["NetFlow(by Volme) (day)"].sum(),
            "IN(Volume)FLOW": subset["INFlow(by Volume) (day)"].sum(),
            "OUT(Volume)FLOW": subset["OutFlow(by Volume) (day)"].sum(),
        }

        if all(v == 0 for v in values.values()):
            print(f"[SKIP] {display_name} all zero flows")
            continue

        file_path = f"{STANDALONE_PATH}/{display_name}(Daily NETFLOW).csv"

        with open(file_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=NETFLOW_FIELDS)
            writer.writerow({"DATE": TODAY, "TICKER": file_ticker, **values})

        print(f"[INFO] Index NetFlow saved → {display_name}")


# =============================================================================
# Main Execution
# =============================================================================
if __name__ == "__main__":
    export_full_netflow_file()
    export_ticker_standalone_files()
    export_index_netflows()
