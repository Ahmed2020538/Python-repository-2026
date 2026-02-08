# -*- coding: utf-8 -*-
"""
Indices and Sectors Watchlists Extractor
----------------------------------------
Extracts indices and sectors symbols from Oracle DB
and saves them as Trading/Amibroker watchlist files (.tls).

Author: Ahmad Elsayed
"""

import pandas as pd
import cx_Oracle
from pathlib import Path


# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
DB_DSN = "STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK"

INDICES_PATH = Path(
    r"C:\AhmedElsayed-Reliable Work Space\Extract Data from DB\Extract_Indicies&Sectors\Indices WLs"
)

SECTORS_PATH = Path(
    r"C:\AhmedElsayed-Reliable Work Space\Extract Data from DB\Extract_Indicies&Sectors\Sectors WLs"
)

INDICES_QUERIES = {
    "EGX30": """
        SELECT C.SYMBOL_CODE, S.REUTERS
        FROM CASE30_COMPANIES C
        LEFT JOIN SYMBOLINFO S
        ON C.SYMBOL_CODE = S.SYMBOL_CODE
        ORDER BY S.REUTERS
    """,
    "EGX50": """
        SELECT E.SYMBOL_CODE, S.REUTERS
        FROM EGX50_SYMBOLS E
        LEFT JOIN SYMBOLINFO S
        ON E.SYMBOL_CODE = S.SYMBOL_CODE
        ORDER BY S.REUTERS
    """,
    "EGX70": """
        SELECT E.SYMBOL_CODE, S.REUTERS
        FROM EGX70_SYMBOLS E
        LEFT JOIN SYMBOLINFO S
        ON E.SYMBOL_CODE = S.SYMBOL_CODE
        ORDER BY S.REUTERS
    """,
    "EGX100": """
        SELECT E.SYMBOL_CODE, S.REUTERS
        FROM EGX100_SYMBOLS E
        LEFT JOIN SYMBOLINFO S
        ON E.SYMBOL_CODE = S.SYMBOL_CODE
        ORDER BY S.REUTERS
    """,
    "EGX30TR": """
        SELECT E.SYMBOL_CODE, S.REUTERS
        FROM EGX30_SYMBOLS_TR E
        LEFT JOIN SYMBOLINFO S
        ON E.SYMBOL_CODE = S.SYMBOL_CODE
        ORDER BY S.REUTERS
    """,
    "EGX_Shariah": """
        SELECT E.SYMBOL_CODE, S.REUTERS
        FROM EGX_SHARIAH_SYMBOLS E
        LEFT JOIN SYMBOLINFO S
        ON E.SYMBOL_CODE = S.SYMBOL_CODE
        ORDER BY S.REUTERS
    """,
    "EGX35-LV": """
        SELECT E.SYMBOL_CODE, S.REUTERS
        FROM EGX_VOLATILITY_SYMBOLS E
        LEFT JOIN SYMBOLINFO S
        ON E.SYMBOL_CODE = S.SYMBOL_CODE
        ORDER BY S.REUTERS
    """
}


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
# Utilities
# ------------------------------------------------------------------
def normalize_reuters_symbol(symbol: str) -> str:
    """
    Remove market suffix from Reuters symbol.
    """
    return symbol.split(".")[0] if isinstance(symbol, str) else symbol


def save_watchlist(symbols: pd.Series, file_path: Path) -> None:
    """
    Save symbols as a .tls watchlist file.
    """
    symbols.to_frame(name="Symbol").set_index("Symbol").to_csv(
        file_path, header=None
    )


# ------------------------------------------------------------------
# Extract Indices Watchlists
# ------------------------------------------------------------------
def extract_indices_watchlists(connection: cx_Oracle.Connection) -> None:
    """
    Extract indices symbols and save them as watchlist files.
    """
    for index_name, query in INDICES_QUERIES.items():
        df = pd.read_sql(query, connection)

        df["REUTERS"] = df["REUTERS"].apply(normalize_reuters_symbol)

        print(f"Index :: {index_name}")
        print(f"Symbols Count :: {df['REUTERS'].count()}")

        save_watchlist(
            df["REUTERS"],
            INDICES_PATH / f"{index_name}.tls"
        )

        print(f"Watchlist for {index_name} created successfully")
        print("-" * 70)


# ------------------------------------------------------------------
# Extract Sectors Watchlists
# ------------------------------------------------------------------
def extract_sectors_watchlists(connection: cx_Oracle.Connection) -> None:
    """
    Extract sector-based symbols and save them as watchlist files.
    """
    sectors = pd.read_sql("SELECT SECTORCODE, SECTOR_DESC FROM STOCK.CASE_SECTOR_INDEX", connection)
    sectors.drop_duplicates(inplace=True)

    symbols = pd.read_sql(
        "SELECT SYMBOL_CODE, SECTOR_ID, ENG_NAME, REUTERS FROM STOCK.SYMBOLINFO",
        connection
    )

    symbols["REUTERS"] = symbols["REUTERS"].apply(normalize_reuters_symbol)
    symbols["SECTOR_ID"] = pd.to_numeric(symbols["SECTOR_ID"])

    merged = symbols.merge(
        sectors,
        how="inner",
        left_on="SECTOR_ID",
        right_on="SECTORCODE"
    )

    for sector_name in merged["SECTOR_DESC"].unique():
        sector_symbols = merged.loc[
            merged["SECTOR_DESC"] == sector_name, "REUTERS"
        ]

        save_watchlist(
            sector_symbols,
            SECTORS_PATH / f"{sector_name}.tls"
        )

        print(f"Sector Watchlist Extracted :: {sector_name}")


# ------------------------------------------------------------------
# Script Entry Point
# ------------------------------------------------------------------
if __name__ == "__main__":

    db_connection = create_db_connection()

    extract_indices_watchlists(db_connection)
    print("All Indices Watchlists Extracted Successfully ✔")
    print("=" * 80)

    extract_sectors_watchlists(db_connection)
    print("All Sectors Watchlists Extracted Successfully ✔")