# -*- coding: utf-8 -*-
"""
Market Breadth Data Processing & Oracle Insertion
Author: Ahmad Elsayed
"""

import os
import csv
import cx_Oracle
import pandas as pd
from datetime import date
from typing import Dict, List


# =========================================================
# Database Utilities
# =========================================================
def get_oracle_connection() -> cx_Oracle.Connection:
    """
    Create and return Oracle DB connection.
    """
    connection = cx_Oracle.connect(
        "STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK"
    )
    print(f"Oracle Version: {connection.version}")
    return connection


# =========================================================
# Date Handling
# =========================================================
def get_target_date() -> str:
    """
    Determine the date used for data retrieval.
    """
    use_today = input("Do u Wanna Retrieve MB Data Today? (y/n): ").lower()

    if use_today == "y":
        target_date = date.today().strftime("%Y-%m-%d")
    else:
        year = input("Set Year  : ")
        month = input("Set Month : ")
        day = input("Set Day   : ")
        target_date = f"{year}-{month}-{day}"

    print(f"Retrieving data for date: {target_date}")
    return target_date


# =========================================================
# File & Data Utilities
# =========================================================
def load_and_filter_excel(path: str, target_date: str) -> pd.DataFrame:
    """
    Load Excel file and filter rows by target date.
    """
    df = pd.read_excel(path)
    return df[df["Date"] == target_date]


def merge_long_short(
    df_long: pd.DataFrame,
    df_short: pd.DataFrame,
    ticker_name: str
) -> pd.DataFrame:
    """
    Merge Long & Short Market Breadth data and calculate net value.
    """
    merged_df = pd.merge(df_long, df_short, on="Date", how="inner")
    merged_df["Long - Short Cout"] = (
        merged_df["Long Count"] - merged_df["Short Count"]
    )
    merged_df["TICKER"] = ticker_name

    return merged_df[
        [
            "Date",
            "TICKER",
            "Long Count",
            "Exit Long Count",
            "Short Count",
            "Exit Short Count",
            "Long - Short Cout",
        ]
    ]


def append_to_csv(file_path: str, df: pd.DataFrame) -> None:
    """
    Append Market Breadth data to CSV file.
    """
    with open(file_path, "a", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "Date",
                "Ticker",
                "Long Count",
                "Exit Long Count",
                "Short Count",
                "Exit Short Count",
                "Long - Short Cout",
            ],
        )

        for _, row in df.iterrows():
            writer.writerow(
                {
                    "Date": row["Date"],
                    "Ticker": row["TICKER"],
                    "Long Count": row["Long Count"],
                    "Exit Long Count": row["Exit Long Count"],
                    "Short Count": row["Short Count"],
                    "Exit Short Count": row["Exit Short Count"],
                    "Long - Short Cout": row["Long - Short Cout"],
                }
            )


# =========================================================
# Oracle Insertion
# =========================================================
def insert_mb_data_to_oracle(csv_path: str) -> None:
    """
    Read last record from CSV and insert into Oracle DB.
    """
    df = pd.read_csv(csv_path)
    df["Ticker"] = df["Ticker"].str.replace(" ", "")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    df.set_index("Date", inplace=True)
    df.sort_index(inplace=True)

    df = df.tail(1)

    connection = get_oracle_connection()
    cursor = connection.cursor()

    for index, row in df.iterrows():
        try:
            record = [
                row["Ticker"],
                row["Long Count"],
                row["Exit Long Count"],
                row["Short Count"],
                row["Exit Short Count"],
                row["Long - Short Cout"],
                index.to_pydatetime(),
                1,
                1,
            ]

            cursor.execute(
                """
                INSERT INTO FILL_OHLCV
                (TICKER, OPEN, HIGH, LOW, CLOSE, VOLUME, BARTIMESTAMP, ASSET, VWAP)
                VALUES
                (:1, :2, :3, :4, :5, :6, :7, :8, :9)
                """,
                record,
            )

            connection.commit()
            print(f"Inserted record: {record}")

        except Exception as exc:
            print(f"Failed to insert record: {record}")
            print(str(exc))


# =========================================================
# Main Workflow
# =========================================================
def main() -> None:
    target_date = get_target_date()

    excel_base_path = (
        "Y:/Asset Management-Data science/PharosQuantitativeTeam/"
        "DataSets-For-Import Wizard/DataFeed Data/myexcelmarketbreadthfiles"
    )

    output_paths = [
        "C:/AhmedElsayed-Reliable Work Space/Insert MB Data in our database",
        "Y:/Asset Management-Data science/PharosQuantitativeTeam/"
        "DataSets-For-Import Wizard/DataFeed Data/myexcelmarketbreadthfiles/"
        "MB-Data-Processing",
    ]

    indices_config: Dict[str, str] = {
        "EGX30": "EGX30LASTEWIMB",
        "EGX50": "EGX50LASTEWIMB",
        "EGX70": "EGX70LASTEWIMB",
        "EGX100": "EGX100LASTEWIMB",
    }

    final_dataframes: List[pd.DataFrame] = []

    for index_name, ticker in indices_config.items():
        df_long = load_and_filter_excel(
            f"{excel_base_path}/MarketBreadth-LongCountOnly-{index_name}.xlsx",
            target_date,
        )
        df_short = load_and_filter_excel(
            f"{excel_base_path}/MarketBreadth-ShortCountOnly-{index_name}.xlsx",
            target_date,
        )

        final_dataframes.append(
            merge_long_short(df_long, df_short, ticker)
        )

    csv_names = [
        "(FinalEGX30MBLong&Short).csv",
        "(FinalEGX50MBLong&Short).csv",
        "(FinalEGX70MBLong&Short).csv",
        "(FinalEGX100MBLong&Short).csv",
    ]

    for path in output_paths:
        for csv_name, df in zip(csv_names, final_dataframes):
            append_to_csv(os.path.join(path, csv_name), df)

    # Insert to Oracle
    oracle_path = output_paths[0]
    for file in os.listdir(oracle_path):
        if file.endswith(".csv"):
            insert_mb_data_to_oracle(os.path.join(oracle_path, file))

    print("=================================")
    print("Market Breadth Data Processing DONE")
    print("=================================")


if __name__ == "__main__":
    main()