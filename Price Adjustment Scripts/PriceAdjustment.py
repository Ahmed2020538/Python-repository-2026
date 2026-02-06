# -*- coding: utf-8 -*-
"""
Price Adjustment Script
-----------------------
Adjusts historical OHLCV prices for a given ticker
before a specified adjustment date.

Created on : Sun Jul 1 02:00:00 2024
Author     : Ahmad Elsayed
"""

import cx_Oracle
from datetime import datetime


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
def create_db_connection() -> cx_Oracle.Connection:
    """
    Establish and return an Oracle database connection.
    """
    connection = cx_Oracle.connect(
        "STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK"
    )
    print(f"Connected to Oracle Database - Version: {connection.version}")
    return connection


# ------------------------------------------------------------------
# Price Adjustment Function
# ------------------------------------------------------------------
def adjust_prices(
    connection: cx_Oracle.Connection,
    ratio: float,
    ticker: str,
    adjustment_date: datetime
) -> None:
    """
    Adjust OHLCV prices by dividing historical values
    by the given ratio before the adjustment date.

    Parameters
    ----------
    connection : cx_Oracle.Connection
        Active database connection
    ratio : float
        Adjustment ratio (old_price / new_price)
    ticker : str
        Stock ticker symbol
    adjustment_date : datetime
        First date AFTER the adjustment
    """

    update_queries = [
        """
        UPDATE STOCK.FILL_OHLCV
        SET
            OPEN  = OPEN  / :ratio,
            HIGH  = HIGH  / :ratio,
            LOW   = LOW   / :ratio,
            CLOSE = CLOSE / :ratio,
            VWAP  = VWAP  / :ratio
        WHERE
            TICKER = :ticker
            AND BARTIMESTAMP < :adjustment_date
        """,
        """
        UPDATE STOCK.FILL_OHLCV_1MIN
        SET
            OPEN  = OPEN  / :ratio,
            HIGH  = HIGH  / :ratio,
            LOW   = LOW   / :ratio,
            CLOSE = CLOSE / :ratio,
            VWAP  = VWAP  / :ratio
        WHERE
            TICKER = :ticker
            AND BARTIMESTAMP < :adjustment_date
        """
    ]

    params = {
        "ratio": ratio,
        "ticker": ticker,
        "adjustment_date": adjustment_date
    }

    cursor = connection.cursor()

    try:
        for query in update_queries:
            cursor.execute(query, params)

        connection.commit()
        print(f"Adjustment for {ticker} completed successfully ✅")

    except Exception as error:
        connection.rollback()
        print("Price adjustment failed ❌")
        print(str(error))


# ------------------------------------------------------------------
# Script Entry Point
# ------------------------------------------------------------------
if __name__ == "__main__":

    adjustment_datetime = datetime(
        ADJUST_YEAR,
        ADJUST_MONTH,
        ADJUST_DAY,
        0,
        0
    )

    db_connection = create_db_connection()

    adjust_prices(
        connection=db_connection,
        ratio=ADJUST_RATIO,
        ticker=TICKER,
        adjustment_date=adjustment_datetime
    )

    print(
        f"Adjustment applied for {TICKER} "
        f"before {ADJUST_YEAR}-{ADJUST_MONTH:02d}-{ADJUST_DAY:02d}"
    )