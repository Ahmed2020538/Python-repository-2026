# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 14:41:08 2019
@author: Ahmed.Montasser
"""

import sys
import time
import datetime
import pandas as pd
import cx_Oracle


# =============================================================================
# Database Connection
# =============================================================================

def db_connect():
    """
    Create a standalone Oracle DB connection.
    Returns:
        cx_Oracle.Connection
    """
    connection = cx_Oracle.connect(
        'STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK'
    )
    print(connection.version)
    return connection


# =============================================================================
# Main Infinite Processing Loop
# =============================================================================

while True:

    # -------------------------------------------------------------------------
    # PART 1: PROCESS TRADES → 1 MIN OHLCV + VWAP
    # -------------------------------------------------------------------------

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
    rows = cursor.fetchmany(numRows=100000)

    trades_df = pd.DataFrame(
        rows,
        columns=["code", "time", "price", "volume"]
    )

    trades_df.set_index("time", inplace=True)
    trades_df.index = pd.to_datetime(trades_df.index)
    trades_df.dropna(inplace=True)

    symbols = trades_df["code"].unique()

    for symbol in symbols:

        symbol_df = trades_df.loc[trades_df["code"] == symbol].copy()

        # VWAP cumulative calculation
        vwap_cumsum = (symbol_df["price"] * symbol_df["volume"]).cumsum()
        volume_cumsum = symbol_df["volume"].cumsum()

        # 1 Minute OHLCV
        ohlc_df = symbol_df["price"].resample("1Min").ohlc()
        ohlc_df["volume"] = symbol_df["volume"].resample("1Min").sum()
        ohlc_df["vwap"] = (vwap_cumsum / volume_cumsum).resample("1Min").last()

        ohlc_df.dropna(inplace=True)

        ticker = symbol.replace(".CA", "").strip()

        # Get last stored bar
        cursor.execute(
            """
            SELECT *
            FROM STOCK.FILL_OHLCV_1MIN
            WHERE Ticker = :ticker
            ORDER BY BARTIMESTAMP DESC
            """,
            [ticker]
        )
        last_row = cursor.fetchone()

        if last_row:
            to_insert = ohlc_df.loc[ohlc_df.index.to_pydatetime() > last_row[6]]
            to_update = ohlc_df.loc[ohlc_df.index.to_pydatetime() == last_row[6]]
        else:
            to_insert = ohlc_df.copy()
            to_update = []

        to_insert.dropna(inplace=True)

        # Update existing bar if volume changed
        if len(to_update) > 0 and last_row[5] != to_update["volume"].iloc[0]:
            cursor.execute(
                """
                UPDATE STOCK.FILL_OHLCV_1MIN
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
                ]
            )

        # Insert new bars
        for ts, row in to_insert.iterrows():
            try:
                cursor.execute(
                    """
                    INSERT INTO FILL_OHLCV_1MIN
                    (TICKER, OPEN, HIGH, LOW, CLOSE, VOLUME,
                     BARTIMESTAMP, ASSET, VWAP)
                    VALUES (:stock, :open, :high, :low, :close,
                            :vol, :time, :1, :2)
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
                    ]
                )
                con.commit()
            except Exception as e:
                print(str(e))

    con.close()

    # -------------------------------------------------------------------------
    # PART 2: PROCESS INDICES (5 MIN OHLC)
    # -------------------------------------------------------------------------

    con = db_connect()
    cursor = con.cursor()

    cursor.execute("SELECT * FROM CASEINDEX")
    rows = cursor.fetchmany(numRows=100000)

    index_df = pd.DataFrame(rows, columns=["time", "code", "price"])
    index_df.set_index("time", inplace=True)
    index_df.index = pd.to_datetime(index_df.index)
    index_df.dropna(inplace=True)

    indices = index_df["code"].unique()

    try:
        for index_name in indices:

            clean_name = index_name.replace("EWI", "").strip()

            index_sql_map = {
                "EGX30": "CASE30_COMPANIES",
                "EGX70": "EGX70_SYMBOLS_EWI",
                "EGX100": "EGX100_SYMBOLS",
                "EGX50": "EGX50_SYMBOLS",
                "EGX30 Capped": "EGX30_CAP_SYMBOLS",
                "SHARIAH": "EGX_SHARIAH_SYMBOLS",
                "EGX35-LV": "EGX_VOLATILITY_SYMBOLS",
            }

            if clean_name == "EGX30 TR":
                clean_name = "EGX30TR"

            if clean_name not in index_sql_map:
                continue

            cursor.execute(
                f"""
                SELECT T2.REUTERS
                FROM {index_sql_map[clean_name]} T1
                JOIN STOCK.SYMBOLINFO T2
                    ON T2.SYMBOL_CODE = T1.SYMBOL_CODE
                """
            )

            symbols = [s[0].replace(".CA", "").strip() for s in cursor.fetchall()]
            placeholders = ",".join(f":{i}" for i in range(len(symbols)))

            cursor.execute(
                f"""
                SELECT BARTIMESTAMP, SUM(VOLUME)
                FROM STOCK.FILL_OHLCV_1MIN
                WHERE Ticker IN ({placeholders})
                GROUP BY BARTIMESTAMP
                ORDER BY BARTIMESTAMP DESC
                """,
                symbols,
            )

            volume_df = pd.DataFrame(
                cursor.fetchall(),
                columns=["time", "volume"]
            )
            volume_df.set_index("time", inplace=True)
            volume_df.index = pd.to_datetime(volume_df.index)

            ohlc_5m = index_df.loc[
                index_df["code"] == index_name, "price"
            ].resample("5Min").ohlc()

            final_df = pd.concat([ohlc_5m, volume_df], axis=1)
            final_df.dropna(inplace=True)

            cursor.execute(
                """
                SELECT *
                FROM STOCK.FILL_OHLCV_1MIN
                WHERE Ticker = :name
                ORDER BY BARTIMESTAMP DESC
                """,
                [clean_name],
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
                    INSERT INTO FILL_OHLCV_1MIN
                    (TICKER, OPEN, HIGH, LOW, CLOSE,
                     VOLUME, BARTIMESTAMP, ASSET, VWAP)
                    VALUES (:stock, :open, :high, :low,
                            :close, :vol, :time, :1, :2)
                    """,
                    [
                        clean_name,
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        row["volume"],
                        ts.to_pydatetime(),
                        0,
                        row["close"],
                    ]
                )
                con.commit()

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(str(e), exc_tb.tb_lineno)

    con.close()
    time.sleep(30)
