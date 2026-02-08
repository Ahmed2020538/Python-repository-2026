# -*- coding: utf-8 -*-
"""
EWI Last Indexes Loader
----------------------
Deletes existing EWI Last Index data from FILL_OHLCV table
and reloads adjusted values from CSV files.

Author: Ahmad Elsayed
"""

import pandas as pd
import cx_Oracle
from datetime import datetime
from pathlib import Path


# ------------------------------------------------------------------
# Database Configuration
# ------------------------------------------------------------------
DB_DSN = "STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK"


# ------------------------------------------------------------------
# Database Connection
# ------------------------------------------------------------------
def create_db_connection() -> cx_Oracle.Connection:
    """
    Create and return an Oracle database connection.
    """
    connection = cx_Oracle.connect(DB_DSN)
    print(f"Connected to Oracle DB - Version: {connection.version}")
    return connection


# ------------------------------------------------------------------
# Delete Existing EWI Last Index Data
# ------------------------------------------------------------------
def delete_ewi_last_indexes(connection: cx_Oracle.Connection) -> None:
    """
    Delete all EWI Last Index tickers from FILL_OHLCV table.
    """
    ewi_tickers = [
        "EGX30LASTEWI",
        "EGX50LASTEWI",
        "EGX70LASTEWI",
        "EGX100LASTEWI"
    ]

    cursor = connection.cursor()
    delete_sql = "DELETE FROM FILL_OHLCV WHERE TICKER = :ticker"

    for ticker in ewi_tickers:
        cursor.execute(delete_sql, {"ticker": ticker})
        connection.commit()
        print(f"Ticker {ticker} deleted from FILL_OHLCV")

    print("EWI Last Index cleanup completed.")


# ------------------------------------------------------------------
# Load and Prepare CSV Data
# ------------------------------------------------------------------
def load_and_prepare_csv(file_path: Path) -> pd.DataFrame:
    """
    Load CSV file and prepare OHLCV data for insertion.
    """
    df = pd.read_csv(file_path)

    price_columns = ["OPEN", "HIGH", "LOW", "CLOSE", "VWAP"]
    df[price_columns] = df[price_columns] * 1000

    df = df[
        ["TICKER", "OPEN", "HIGH", "LOW", "CLOSE",
         "VOLUME", "BARTIMESTAMP", "ASSET", "VWAP"]
    ]

    df["BARTIMESTAMP"] = pd.to_datetime(
        df["BARTIMESTAMP"], errors="coerce", utc=True
    )

    df.set_index("BARTIMESTAMP", inplace=True)
    df.sort_index(inplace=True)

    return df


# ------------------------------------------------------------------
# Insert Data into Oracle
# ------------------------------------------------------------------
def insert_fill_ohlcv(
    connection: cx_Oracle.Connection,
    dataframe: pd.DataFrame
) -> None:
    """
    Insert prepared OHLCV data into FILL_OHLCV table.
    """
    insert_sql = """
        INSERT INTO FILL_OHLCV
        (TICKER, OPEN, HIGH, LOW, CLOSE, VOLUME, BARTIMESTAMP, ASSET, VWAP)
        VALUES
        (:1, :2, :3, :4, :5, :6, :7, :8, :9)
    """

    cursor = connection.cursor()

    for timestamp, row in dataframe.iterrows():
        try:
            values = [
                row["TICKER"],
                row["OPEN"],
                row["HIGH"],
                row["LOW"],
                row["CLOSE"],
                row["VOLUME"],
                timestamp.to_pydatetime(),
                1,
                row["VWAP"]
            ]

            cursor.execute(insert_sql, values)
            connection.commit()

        except Exception as error:
            connection.commit()
            print("Insert failed for row:")
            print(values)
            print(str(error))


# ------------------------------------------------------------------
# Script Entry Point
# ------------------------------------------------------------------
if __name__ == "__main__":

    base_path = Path(
        r"C:\AhmedElsayed-Reliable Work Space\Handle_EWI_last_Indexes_after_adjustment"
    )

    csv_files = [
        "first-row-30.csv",
        "first-row-50.csv",
        "first-row-70.csv",
        "first-row-100.csv"
    ]

    db_connection = create_db_connection()

    # Step 1: Delete old EWI Last Index data
    delete_ewi_last_indexes(db_connection)

    # Step 2: Load and insert adjusted data
    for file_name in csv_files:
        file_path = base_path / file_name

        df = load_and_prepare_csv(file_path)
        insert_fill_ohlcv(db_connection, df)

        print(f"Insert completed for file: {file_name}")
        print("-" * 90)