# -*- coding: utf-8 -*-
"""
Price Adjustment Script
-----------------------
Adjusts OHLCV prices for a specific ticker before a given adjustment date.

Created on: Sun Jul 1 02:00:00 2024
Author: Ahmad Elsayed
"""

import cx_Oracle
import datetime


# ------------------------------------------------------------------
# User Inputs
# ------------------------------------------------------------------
ADJUST_RATIO = float(input("Set ticker ratio value (e.g. 1.25): "))
TICKER       = input("Set ticker name (e.g. COMI): ").strip().upper()
ADJUST_YEAR  = int(input("Set adjustment year (YYYY): "))
ADJUST_MONTH = int(input("Set adjustment month (MM): "))
ADJUST_DAY   = int(input("Set adjustment day (DD): "))


# ------------------------------------------------------------------
# Database Connection
# ------------------------------------------------------------------
def get_db_connection() -> cx_Oracle.Connection:
    """
    Create and return an Oracle database connection.
    """
    connection = cx_Oracle.connect(
        "STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK"
    )
    print(f"Connected to Oracle DB - Version: {connection.version}")
    return connection


# ------------------------------------------------------------------
# Price Adjustment Logic
# ------------------------------------------------------------------
def adjust_prices(
    connection: cx_Oracle.Connection,
    ratio: float,
    ticker: str,
    adjust_date: datetime.datetime
) -> None:
    """
    Adjust OHLCV prices for a specific ticker before a given date.

    Parameters
    ----------
    connection : cx_Oracle.Connection
        Active Oracle DB connection
    ratio : float
        Price adjustment ratio (new_price / old_price)
    ticker : str
        Stock ticker symbol
    adjust_date : datetime.datetime
        First date AFTER the adjustment
    """

    update_queries = [
        """
        UPDATE STOCK.FILL_OHLCV
        SET
            OPEN  = OPEN  * :ratio,
            HIGH  = HIGH  * :ratio,
            LOW   = LOW   * :ratio,
            CLOSE = CLOSE * :ratio,
            VWAP  = VWAP  * :ratio
        WHERE
            TICKER = :ticker
            AND BARTIMESTAMP < :adjust_date
        """,
        """
        UPDATE STOCK.FILL_OHLCV_1MIN
        SET
            OPEN  = OPEN  * :ratio,
            HIGH  = HIGH  * :ratio,
            LOW   = LOW   * :ratio,
            CLOSE = CLOSE * :ratio,
            VWAP  = VWAP  * :ratio
        WHERE
            TICKER = :ticker
            AND BARTIMESTAMP < :adjust_date
        """
    ]

    params = {
        "ratio": ratio,
        "ticker": ticker,
        "adjust_date": adjust_date
    }

    cursor = connection.cursor()

    try:
        for query in update_queries:
            cursor.execute(query, params)

        connection.commit()
        print("Price adjustment committed successfully.")

    except Exception as error:
        connection.rollback()
        print("Price adjustment failed!")
        print(str(error))


# ------------------------------------------------------------------
# Script Entry Point
# ------------------------------------------------------------------
if __name__ == "__main__":

    adjustment_date = datetime.datetime(
        ADJUST_YEAR,
        ADJUST_MONTH,
        ADJUST_DAY,
        0,
        0
    )

    db_connection = get_db_connection()

    adjust_prices(
        connection=db_connection,
        ratio=ADJUST_RATIO,
        ticker=TICKER,
        adjust_date=adjustment_date
    )

    print(
        f"Adjustment for {TICKER} before "
        f"{ADJUST_YEAR}-{ADJUST_MONTH:02d}-{ADJUST_DAY:02d} completed ✅"
    )