# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 14:41:08 2022
@author: Ahmed Elsayed Ibrahim
"""

import pandas as pd
import cx_Oracle
import time


# --------------------------------------------------
# Database Connection
# --------------------------------------------------
def db_connect():
    """
    Establish a standalone connection with the Oracle database.

    Returns:
        cx_Oracle.Connection: Active database connection
    """
    connection = cx_Oracle.connect(
        'STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK'
    )
    print(connection.version)
    return connection


# --------------------------------------------------
# Sector Name Normalization
# --------------------------------------------------
def normalize_sector_codes(df):
    """
    Normalize sector names to match database ticker conventions.
    """
    replacements = {
        'Shipping&TransportationServices': 'Shipping&Transportation',
        'IndustrialGoods,ServicesandAutomobiles': 'Indust.Goods,&Automobiles',
        'IT,Media&CommunicationServices': 'IT,Media&Comm',
        'Contracting&ConstructionEngineering': 'Contracting&Construction'
    }

    df['SECTOR_CODE'] = df['SECTOR_CODE'].replace(replacements)

    for char in [',', ' ', '&', '-']:
        df['SECTOR_CODE'] = df['SECTOR_CODE'].str.replace(char, '', regex=False)

    return df


# --------------------------------------------------
# Main Loop
# --------------------------------------------------
while True:

    connection = db_connect()
    cursor = connection.cursor()

    # Fetch sector index data
    sql_sectors = """
        SELECT
            REPLACE(SECTOR_DESC,' ','') AS SECTOR_CODE,
            INDEXTIME,
            INDEXVALUE
        FROM CASE_SECTOR_INDEX
    """
    df_sectors = pd.read_sql(sql_sectors, connection)

    # Normalize sector codes
    df_sectors = normalize_sector_codes(df_sectors)

    # Prepare dataframe
    df_sectors.set_index('INDEXTIME', inplace=True)
    df_sectors.index = pd.to_datetime(df_sectors.index)
    df_sectors.dropna(inplace=True)

    sector_list = df_sectors['SECTOR_CODE'].unique()

    # --------------------------------------------------
    # Process each sector
    # --------------------------------------------------
    for sector in sector_list:

        # Resample to 5-minute OHLC
        ohlc_df = (
            df_sectors[df_sectors['SECTOR_CODE'] == sector]['INDEXVALUE']
            .resample('5Min')
            .ohlc()
        )

        # Get last stored bar
        sql_last_bar = """
            SELECT *
            FROM STOCK.FILL_OHLCV
            WHERE Ticker = :ticker
            ORDER BY BARTIMESTAMP DESC
        """
        cursor.execute(sql_last_bar, [sector.upper()])
        last_record = cursor.fetchone()

        if last_record:
            bars_to_insert = ohlc_df[ohlc_df.index > last_record[6]]
            bars_to_update = ohlc_df[ohlc_df.index == last_record[6]]
        else:
            bars_to_insert = ohlc_df.copy()
            bars_to_update = []

        bars_to_insert.dropna(inplace=True)

        # Update existing bar
        if len(bars_to_update) > 0:
            update_sql = """
                UPDATE STOCK.FILL_OHLCV
                SET OPEN=:1, HIGH=:2, LOW=:3, CLOSE=:4, VOLUME=:5
                WHERE Ticker=:6 AND BARTIMESTAMP=:7
            """
            cursor.execute(
                update_sql,
                [
                    bars_to_update['open'].values[0],
                    bars_to_update['high'].values[0],
                    bars_to_update['low'].values[0],
                    bars_to_update['close'].values[0],
                    0,
                    sector.upper(),
                    bars_to_update.index.to_pydatetime()[0]
                ]
            )

        # Insert new bars
        if len(bars_to_insert) > 0:
            for timestamp, row in bars_to_insert.iterrows():
                try:
                    insert_data = [
                        sector.upper(),
                        row['open'],
                        row['high'],
                        row['low'],
                        row['close'],
                        0,
                        timestamp.to_pydatetime(),
                        0,
                        0
                    ]

                    cursor.execute(
                        """
                        INSERT INTO FILL_OHLCV
                        (TICKER, OPEN, HIGH, LOW, CLOSE, VOLUME,
                         BARTIMESTAMP, ASSET, VWAP)
                        VALUES (:stock, :open, :high, :low, :close,
                                :vol, :time, :1, :2)
                        """,
                        insert_data
                    )

                    connection.commit()
                    print(sector.upper())

                except Exception as error:
                    print(str(error))

    connection.close()
    time.sleep(30)
