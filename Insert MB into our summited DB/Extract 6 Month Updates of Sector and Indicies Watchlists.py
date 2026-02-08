# Load Requairies Package:
#-------------------------
import pandas as pd
import cx_Oracle
import datetime
import csv
import time
import sys
sqllists = ["""SELECT CASE30_COMPANIES.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                    from CASE30_COMPANIES LEFT JOIN SYMBOLINFO
                    ON CASE30_COMPANIES.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                    ORDER BY SYMBOLINFO.REUTERS """, 
            
            """SELECT EGX50_SYMBOLS.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                    from EGX50_SYMBOLS LEFT JOIN SYMBOLINFO
                    ON EGX50_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                    ORDER BY SYMBOLINFO.REUTERS """, 
            
            """SELECT EGX70_SYMBOLS.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                    from EGX70_SYMBOLS LEFT JOIN SYMBOLINFO
                    ON EGX70_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                    ORDER BY SYMBOLINFO.REUTERS """,
            
            """SELECT EGX100_SYMBOLS.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                    from EGX100_SYMBOLS LEFT JOIN SYMBOLINFO
                    ON EGX100_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                    ORDER BY SYMBOLINFO.REUTERS """, 
            
            """SELECT EGX30_SYMBOLS_TR.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                    from EGX30_SYMBOLS_TR LEFT JOIN SYMBOLINFO
                    ON EGX30_SYMBOLS_TR.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                    ORDER BY SYMBOLINFO.REUTERS """,
            
            """SELECT EGX_SHARIAH_SYMBOLS.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                    from EGX_SHARIAH_SYMBOLS LEFT JOIN SYMBOLINFO
                    ON EGX_SHARIAH_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                    ORDER BY SYMBOLINFO.REUTERS """,
           
           
            """SELECT EGX_VOLATILITY_SYMBOLS.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                    from EGX_VOLATILITY_SYMBOLS LEFT JOIN SYMBOLINFO
                    ON EGX_VOLATILITY_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                    ORDER BY SYMBOLINFO.REUTERS """]








indiceslists = ["EGX30","EGX50","EGX70","EGX100","EGX30TR","EGX_Shariah","EGX35-LV"]

con = cx_Oracle.connect('STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK')
cursor = con.cursor()  
for (sqlquary,indexname) in  zip(sqllists, indiceslists):
        df_sectors = pd.read_sql(sqlquary, con)
        df_sectors.REUTERS = df_sectors.REUTERS.apply(lambda x : x.split(".")[0])
        print(f"Index Name :: || {indexname} ||")
        print(f"{indexname} Symbols count :: ( {df_sectors.REUTERS.count()} )")
        #print(f"Symbols :: {df_sectors.REUTERS.unique()}")
        path = 'C:\AhmedElsayed-Reliable Work Space\Extract Data from DB\Extract_Indicies&Sectors\Indices WLs'
        df_sectors.to_csv(f"{path}/{indexname}.tls")
        df_sectors = pd.read_csv(f"{path}/{indexname}.tls")
        #print(df_sectors.columns)
        df_sectors = df_sectors[["REUTERS"]]
        df_sectors.set_index("REUTERS" , inplace=True)
        df_sectors.to_csv(f"{path}/{indexname}.tls",  header=None)
        print(f"Extract All Symbols of {indexname} From Database and Insert them into them Watchlist done (^ - ^)")
        print("----------------------------------------------------------")
        print("----------------------------------------------------------")
print("Extract Indices WatchLists From Database and Save them in the Path done (^ th-nks ^)")
print("------------------------------------------------------------------------------------")
print("------------------------------------------------------------------------------------")
con = cx_Oracle.connect('STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK')
sql="SELECT * FROM  STOCK.CASE_SECTOR_INDEX"
cursor = con.cursor()     
sectors = pd.read_sql(sql, con)
sectors = sectors.sort_values(by = 'SECTORCODE', ascending = True) 
sectors.SECTORCODE.unique()
# to create lookup table for the sectors names with the ID 
sectors = sectors[["SECTORCODE","SECTOR_DESC"]]
#SECTORCODE.drop_duplicates(inplace = True)
sectors.set_index('SECTORCODE',inplace = True)
sectors.drop_duplicates(inplace =True)
sql="SELECT * FROM  STOCK.SYMBOLINFO"
cursor = con.cursor()     
All_symbols = pd.read_sql(sql, con)
All_symbols = All_symbols[["SYMBOL_CODE","SECTOR_ID","ENG_NAME","REUTERS"]]
sectors.reset_index(inplace = True)
All_symbols['SECTOR_ID'] = pd.to_numeric(All_symbols['SECTOR_ID'])
SymbolsWithSectors = All_symbols.merge(sectors,how='inner',left_on="SECTOR_ID",right_on="SECTORCODE")
SymbolsWithSectors["REUTERS"] = SymbolsWithSectors["REUTERS"].apply(lambda x : x.split(".")[0])
seclists = SymbolsWithSectors["SECTOR_DESC"].unique()
path1 ="C:\AhmedElsayed-Reliable Work Space\Extract Data from DB\Extract_Indicies&Sectors\Sectors WLs"
for sect in seclists :
    sectorsdf  = SymbolsWithSectors[SymbolsWithSectors["SECTOR_DESC"] == sect]["REUTERS"].to_csv(f"{path1}/{sect}.tls", header=[ 'Symbol'])
    sectorsdf  = pd.read_csv(f"{path1}/{sect}.tls")
    sectorsdf = sectorsdf[["Symbol"]]
    sectorsdf.set_index("Symbol" , inplace=True)
    sectorsdf.to_csv(f"{path1}/{sect}.tls",  header=None)
    print(f"Extract Sector :: {sect} done")
print("Extract Sectors WatchLists From Database and Save them in the Path done (^ th-nks ^)")
        
        
