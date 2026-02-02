# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 14:41:08 2022
Author: Ahmed Elsayed Ibrahim

Description:
------------
This script aggregates raw trade-level market data into 5-minute OHLCV bars
and stores them in an Oracle database. It also processes market indices using
their constituent symbols and updates index-level OHLCV data accordingly.

The script runs continuously in a fixed time interval.
"""

import sys
import time
import pandas as pd
import cx_Oracle


# =============================================================================
# Database Connection
# =============================================================================

def db_connect():
    """
    Create a standalone Oracle database connection.

    Returns:
        cx_Oracle.Connection
    """
    connection = cx_Oracle.connect(
        "STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK"
    )
    print(connection.version)
    return connection


# =============================================================================
# Main Processing Loop (Runs Forever)
# =============================================================================

while True:

    # =========================================================================
    # PART 1: Trades → 5-Minute OHLCV + VWAP
    # =========================================================================

    con = db_connect()
    cursor = con.cursor()

    trades_sql = """
        SELECT
            T2.REUTERS,
            T1.EXEC_TIME,
            T1.TRADE_PRICE,
            T1.VOLUME_TRADED
        FROM STOCK.TRADES T1
        JOIN STOCK.SYMBOLINFO T2
            ON T2.SYMBOL_CODE = T1.SYMBOL_CODE
    """

    cursor.execute(trades_sql)
    rows = cursor.fetchall()

    trades_df = pd.DataFrame(
        rows, columns=["code", "time", "price", "volume"]
    )

    trades_df.set_index("time", inplace=True)
    trades_df.index = pd.to_datetime(trades_df.index)
    trades_df.dropna(inplace=True)

    symbols = trades_df["code"].unique()

    for symbol in symbols:

        symbol_df = trades_df.loc[trades_df["code"] == symbol].copy()

        # -----------------------------
        # VWAP Calculation
        # -----------------------------
        vwap_cumsum = (symbol_df["price"] * symbol_df["volume"]).cumsum()
        volume_cumsum = symbol_df["volume"].cumsum()

        # -----------------------------
        # 5-Minute OHLCV
        # -----------------------------
        ohlc_df = symbol_df["price"].resample("5Min").ohlc()
        ohlc_df["volume"] = symbol_df["volume"].resample("5Min").sum()
        ohlc_df["vwap"] = (vwap_cumsum / volume_cumsum).resample("5Min").last()
        ohlc_df.dropna(inplace=True)

        ticker = symbol.replace(".CA", "").strip()

        # -----------------------------
        # Fetch Last Stored Bar
        # -----------------------------
        cursor.execute(
            """
            SELECT *
            FROM STOCK.FILL_OHLCV
            WHERE Ticker = :ticker
            ORDER BY BARTIMESTAMP DESC
            """,
            [ticker],
        )
        last_row = cursor.fetchone()

        if last_row:
            to_insert = ohlc_df.loc[
                ohlc_df.index.to_pydatetime() > last_row[6]
            ]
            to_update = ohlc_df.loc[
                ohlc_df.index.to_pydatetime() == last_row[6]
            ]
        else:
            to_insert = ohlc_df.copy()
            to_update = []

        to_insert.dropna(inplace=True)

        # -----------------------------
        # Update Existing Bar (if needed)
        # -----------------------------
        if (
            len(to_update) > 0
            and last_row[5] != to_update["volume"].iloc[0]
            and to_update["volume"].iloc[0] != 0.0
        ):
            cursor.execute(
                """
                UPDATE STOCK.FILL_OHLCV
                SET OPEN=:1, HIGH=:2, LOW=:3, CLOSE=:4, VOLUME=:5
                WHERE Ticker=:6 AND BARTIMESTAMP=:7
                """,
                [
                    to_update["open"].iloc[0],
                    to_update["high"].iloc[0],
                    to_update["low"].iloc[0],
                    to_update["close"].iloc[0],
                    float(to_update["volume"].iloc[0]),
                    ticker,
                    to_update.index.to_pydatetime()[0],
                ],
            )

        # -----------------------------
        # Insert New Bars
        # -----------------------------
        for ts, row in to_insert.iterrows():
            try:
                cursor.execute(
                    """
                    INSERT INTO FILL_OHLCV
                    (TICKER, OPEN, HIGH, LOW, CLOSE,
                     VOLUME, BARTIMESTAMP, ASSET, VWAP)
                    VALUES (:stock, :open, :high, :low,
                            :close, :vol, :time, :1, :2)
                    """,
                    [
                        ticker,
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        row["volume"],
                        ts.to_pydatetime(),
                        1,
                        row["vwap"],
                    ],
                )
                con.commit()
            except Exception as e:
                print(str(e))

    con.close()

    # =========================================================================
    # PART 2: Index Processing (5-Minute OHLCV)
    # =========================================================================

    con = db_connect()
    cursor = con.cursor()

    cursor.execute("SELECT * FROM CASEINDEX")
    rows = cursor.fetchmany(numRows=100000)

    index_df = pd.DataFrame(
        rows, columns=["time", "code", "price"]
    )

    index_df.set_index("time", inplace=True)
    index_df.index = pd.to_datetime(index_df.index)
    index_df.dropna(inplace=True)

    indices = index_df["code"].unique()

    try:
        for index_code in indices:

            index_name = index_code.replace("EWI", "").strip()
            print(index_name)

            index_sql_map = {
                "EGX30": "CASE30_COMPANIES",
                "EGX70": "EGX70_SYMBOLS_EWI",
                "EGX100": "EGX100_SYMBOLS",
                "EGX50": "EGX50_SYMBOLS",
                "EGX30 Capped": "EGX30_CAP_SYMBOLS",
                "SHARIAH": "EGX_SHARIAH_SYMBOLS",
                "EGX35-LV": "EGX_VOLATILITY_SYMBOLS",
            }

            if index_name == "EGX30 TR":
                index_name = "EGX30TR"

            if index_name not in index_sql_map:
                continue

            cursor.execute(
                f"""
                SELECT T2.REUTERS
                FROM {index_sql_map[index_name]} T1
                JOIN STOCK.SYMBOLINFO T2
                    ON T2.SYMBOL_CODE = T1.SYMBOL_CODE
                """
            )

            symbols = [
                s[0].replace(".CA", "").strip()
                for s in cursor.fetchmany(numRows=110)
            ]

            placeholders = ",".join(f":{i}" for i in range(len(symbols)))

            cursor.execute(
                f"""
                SELECT BARTIMESTAMP, SUM(VOLUME)
                FROM STOCK.FILL_OHLCV
                WHERE Ticker IN ({placeholders})
                GROUP BY BARTIMESTAMP
                ORDER BY BARTIMESTAMP DESC
                """,
                symbols,
            )

            volume_df = pd.DataFrame(
                cursor.fetchmany(numRows=10000),
                columns=["time", "volume"],
            )

            volume_df.set_index("time", inplace=True)
            volume_df.index = pd.to_datetime(volume_df.index)

            ohlc_5m = index_df.loc[
                index_df["code"] == index_code, "price"
            ].resample("5Min").ohlc()

            final_df = pd.concat(
                [ohlc_5m, volume_df], axis=1
            ).dropna()

            cursor.execute(
                """
                SELECT *
                FROM STOCK.FILL_OHLCV
                WHERE Ticker = :name
                ORDER BY BARTIMESTAMP DESC
                """,
                [index_name],
            )
            last_row = cursor.fetchone()

            if last_row:
                to_insert = final_df.loc[
                    final_df.index.to_pydatetime() > last_row[6]
                ]
            else:
                to_insert = final_df.copy()

            for ts, row in to_insert.iterrows():
                cursor.execute(
                    """
                    INSERT INTO FILL_OHLCV
                    (TICKER, OPEN, HIGH, LOW, CLOSE,
                     VOLUME, BARTIMESTAMP, ASSET, VWAP)
                    VALUES (:stock, :open, :high, :low,
                            :close, :vol, :time, :1, :2)
                    """,
                    [
                        index_name,
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        row["volume"],
                        ts.to_pydatetime(),
                        0,
                        row["close"],
                    ],
                )
                con.commit()
                print(f"Data inserted at {ts}")

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(str(e), exc_tb.tb_lineno)

    con.close()
    time.sleep(30)
