# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 14:41:08 2022
@author: Ahmed Elsayed Ibrahim
"""

import pandas as pd
import cx_Oracle
import time


# ==================================================
# Database Connection
# ==================================================
def db_connect():
    """
    Creates a standalone connection with the Oracle database.
    """
    connection = cx_Oracle.connect(
        'STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK'
    )
    print(connection.version)
    return connection


# ==================================================
# Index Constituents Fetcher
# ==================================================
def index_constituents_symbols(sql_query, db_connection):
    """
    Fetch index constituents symbols from database.

    Returns:
        list[str]: List of index constituent symbols (Reuters format)
    """
    cursor = db_connection.cursor()
    cursor.execute(sql_query)
    data = cursor.fetchall()
    symbols = [i.strip() for x in data for i in x]
    return symbols


# ==================================================
# EW Index Live Update Engine
# ==================================================
def live_update_ewi_last(db_con, df_all_prices, index_symbols, last_bar_timestamp, price_columns):
    """
    Build and update Equal Weighted Index (EWI) values.

    Logic preserved exactly as original implementation.
    """

    df_ewi = pd.DataFrame()
    df_fillna = pd.DataFrame()
    cursor = db_con.cursor()

    for symbol in index_symbols:

        # ------------------------------------------
        # Previous prices (for filling missing data)
        # ------------------------------------------
        prev_prices_query = """
            SELECT bartimestamp,ticker,open,high,low,close,vwap
            FROM STOCK.FILL_OHLCV
            WHERE ticker =:1 AND bartimestamp <=:2
            ORDER BY bartimestamp DESC
        """
        cursor.execute(prev_prices_query, [str(symbol), last_bar_timestamp])

        prev_prices = pd.DataFrame(
            cursor.fetchone(),
            ['BARTIMESTAMP','TICKER','OPEN','HIGH','LOW','CLOSE','VWAP']
        ).T

        df_fillna = df_fillna.append(prev_prices)

        # ------------------------------------------
        # Prices used for EWI calculation
        # ------------------------------------------
        select_query = """
            SELECT bartimestamp,ticker,open,high,low,close,vwap,volume
            FROM STOCK.FILL_OHLCV
            WHERE ticker = :1 AND bartimestamp >= :2
            ORDER BY bartimestamp DESC
        """

        params = [str(symbol), last_bar_timestamp]
        df_symbol = pd.read_sql(
            select_query,
            db_con,
            index_col='BARTIMESTAMP',
            params=params
        )

        df_all_prices = df_all_prices.append(df_symbol)
        df_all_prices.sort_index(ascending=True, inplace=True)

    df_fillna.set_index('TICKER', inplace=True)

    if not df_all_prices.empty:

        # ------------------------------------------
        # Build full timestamp x ticker grid
        # ------------------------------------------
        df_all_dates = pd.DataFrame(
            index=pd.MultiIndex.from_product(
                [df_all_prices.index.unique(), index_symbols],
                names=['BARTIMESTAMP', 'TICKER']
            )
        )

        df_all_prices = df_all_prices.set_index(
            pd.MultiIndex.from_arrays(
                [df_all_prices.index, df_all_prices['TICKER']],
                names=['BARTIMESTAMP', 'TICKER']
            )
        )
        del df_all_prices['TICKER']

        df_total = pd.merge(
            df_all_dates,
            df_all_prices,
            how='outer',
            left_index=True,
            right_index=True
        )

        df_total.sort_index(ascending=True, inplace=True)

        # ------------------------------------------
        # Fill missing prices
        # ------------------------------------------
        df_total[price_columns] = df_total[price_columns].groupby(
            level='TICKER'
        ).transform(lambda x: x.fillna(method='ffill'))

        df_total[price_columns] = df_total[price_columns].groupby(
            level='TICKER'
        ).transform(lambda x: x.fillna(method='bfill'))

        df_total[price_columns] = df_total[price_columns].fillna(df_fillna)
        df_total['VOLUME'] = df_total['VOLUME'].fillna(0)

        df_total.reset_index(level='TICKER', drop=False, inplace=True)

        # ------------------------------------------
        # Equal Weighted Index Calculation
        # ------------------------------------------
        scale = 1
        df_ewi = (1 / len(index_symbols)) * df_total[price_columns]
        df_ewi = df_ewi.groupby('BARTIMESTAMP').sum() * scale
        df_ewi['VOLUME'] = df_total['VOLUME'].groupby('BARTIMESTAMP').sum()

    return df_ewi


# ==================================================
# Initialization
# ==================================================
con = db_connect()

sql30 = "SELECT REPLACE(T2.REUTERS,'.CA','') FROM CASE30_COMPANIES T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE ORDER BY T2.REUTERS"
sql70 = "SELECT REPLACE(T2.REUTERS,'.CA','') FROM EGX70_SYMBOLS T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE ORDER BY T2.REUTERS"
sql100 = "SELECT REPLACE(T2.REUTERS,'.CA','') FROM EGX100_SYMBOLS T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE ORDER BY T2.REUTERS"
sql34SHARIAH = "SELECT REPLACE(T2.REUTERS,'.CA','') FROM EGX_SHARIAH_SYMBOLS T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE ORDER BY T2.REUTERS"
sql35_LV = "SELECT REPLACE(T2.REUTERS,'.CA','') FROM EGX_VOLATILITY_SYMBOLS T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE ORDER BY T2.REUTERS"

egx30_symbols = index_constituents_symbols(sql30, con)
egx70_symbols = index_constituents_symbols(sql70, con)
egx100_symbols = index_constituents_symbols(sql100, con)
egx34Shariah_symbols = index_constituents_symbols(sql34SHARIAH, con)
egx35_LV_symbols = index_constituents_symbols(sql35_LV, con)

egx50_symbols = [
    "ABUK","ADIB","AMOC","ARCC","BTFH","CCAP","COMI","EAST","EFID","EFIH",
    "EGAL","EGCH","EMFD","ETEL","FWRY","GBCO","HELI","HRHO","ISPH","JUFO",
    "MCQE","OIH","ORAS","ORHD","ORWE","PHDC","RAYA","RMDA","TMGH","VLMR",
    "VLMRA","MTIE","OLFI","PHAR","POUL","PRDC","SAUD","SKPC","SUGR","SWDY",
    "TALM","ZMID","MPCO","MASR","CIEB","ALCN","DSCW","OFH","COSG","OCDI"
]

all_indices_symbols = [
    egx30_symbols,
    egx70_symbols,
    egx100_symbols,
    egx50_symbols,
    egx35_LV_symbols,
    egx34Shariah_symbols
]

price_columns = ['OPEN','HIGH','LOW','CLOSE','VWAP']

ewi_names = [
    'EGX30LASTEWI',
    'EGX70LASTEWI',
    'EGX100LASTEWI',
    'EGX50LASTEWI',
    'EGX35-LVLASTEWI',
    'EGX34SHARIAHLASTEWI'
]


# ==================================================
# Main Loop
# ==================================================
while True:

    con = db_connect()
    cursor = con.cursor()

    for i in range(len(ewi_names)):

        # ------------------------------------------
        # Fetch last bar
        # ------------------------------------------
        last_bar_query = """
            SELECT BARTIMESTAMP,VOLUME,OPEN,HIGH,LOW,CLOSE
            FROM STOCK.FILL_OHLCV
            WHERE TICKER = :1
            ORDER BY BARTIMESTAMP DESC
        """
        cursor.execute(last_bar_query, [ewi_names[i]])
        last_bar = cursor.fetchone()

        all_prices = pd.DataFrame()

        df_ewi = live_update_ewi_last(
            con,
            all_prices,
            all_indices_symbols[i],
            last_bar[0],
            price_columns
        )

        if not df_ewi.empty:
            tb_insert = df_ewi[df_ewi.index > last_bar[0]]
            tb_update = df_ewi[df_ewi.index == last_bar[0]]
        else:
            tb_insert = df_ewi.copy()
            tb_update = []

        # ------------------------------------------
        # Update
        # ------------------------------------------
        if len(tb_update) > 0 and tb_update['VOLUME'][0] != last_bar[1]:
            update_query = """
                UPDATE STOCK.FILL_OHLCV
                SET VOLUME=:1
                WHERE Ticker = :2 AND BARTIMESTAMP=:3
            """
            cursor.execute(
                update_query,
                [
                    tb_update['VOLUME'].values[0].astype(float),
                    ewi_names[i],
                    tb_update.index.to_pydatetime()[0]
                ]
            )
            print(ewi_names[i], 'is being updated')
        else:
            print('...')

        # ------------------------------------------
        # Insert
        # ------------------------------------------
        if len(tb_insert) > 0:
            for index, row in tb_insert.iterrows():
                try:
                    line = [0]*9

                    if ewi_names[i] in ['EGX34SHARIAHLASTEWI', 'EGX35-LVLASTEWI']:
                        line[0] = ewi_names[i]
                        line[1] = row['OPEN']
                        line[2] = row['HIGH']
                        line[3] = row['LOW']
                        line[4] = row['CLOSE']
                        line[5] = row['VOLUME']
                        line[6] = index.to_pydatetime()
                        line[7] = 0
                        line[8] = row['VWAP']
                    else:
                        line[0] = ewi_names[i]
                        line[1] = row['OPEN'] * 1000
                        line[2] = row['HIGH'] * 1000
                        line[3] = row['LOW']  * 1000
                        line[4] = row['CLOSE'] * 1000
                        line[5] = row['VOLUME']
                        line[6] = index.to_pydatetime()
                        line[7] = 0
                        line[8] = row['VWAP'] * 1000

                    cursor.execute(
                        """
                        INSERT INTO FILL_OHLCV
                        (TICKER,OPEN,HIGH,LOW,CLOSE,VOLUME,BARTIMESTAMP,ASSET,VWAP)
                        VALUES (:stock,:open,:high,:low,:close,:vol,:time,:1,:2)
                        """,
                        line
                    )

                    con.commit()
                    print(ewi_names[i], 'is being inserted')
                    print(line)

                except Exception as e:
                    print(str(e))
        else:
            print("...")

    con.close()
    time.sleep(60)
