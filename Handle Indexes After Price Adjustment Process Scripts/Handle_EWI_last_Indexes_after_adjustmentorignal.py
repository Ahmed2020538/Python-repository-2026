# Load reqiured package :
import pandas as pd
import cx_Oracle
import datetime
import csv
import time
import sys
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
# Access on Orcale DataBase :
def dbConnect():
    '''
    creates a standalone connection with the database
    parameters:
        none
        
    return: 
       con: cx_oracle connection
    '''
    
    con = cx_Oracle.connect('STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK')
    print (con.version)
    return con
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
# Delet Data From FILL_OHLCV Table
con=dbConnect()
cur = con.cursor()
EWIlastIndexes_namelist = ['EGX30LASTEWI', 'EGX50LASTEWI', 'EGX70LASTEWI', 'EGX100LASTEWI']
for indexname in EWIlastIndexes_namelist : 
    statement = 'delete from FILL_OHLCV where TICKER = :TICKER'
    cur.execute(statement, {'TICKER':indexname})
    con.commit()
    print(f"The Ticker :: {indexname} Deleted From FILL_OHLCV Table done")
print("The First Process Done (Delet all EWI Last Indicies)")
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
exlfiles = ['first-row-30.csv', 'first-row-50.csv', 'first-row-70.csv', 'first-row-100.csv']
for file in exlfiles :
    path = f"C:\AhmedElsayed-Reliable Work Space\Handle_EWI_last_Indexes_after_adjustment/{file}"

    tbIns = pd.read_csv(path)
    tbIns["OPEN"]  = tbIns["OPEN"] *1000
    tbIns["HIGH"]  = tbIns["HIGH"] *1000
    tbIns["LOW"]   = tbIns["LOW"]  *1000
    tbIns["CLOSE"] = tbIns["CLOSE"]*1000
    tbIns["VWAP"]  = tbIns["VWAP"] *1000
    tbIns =tbIns[["TICKER", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "BARTIMESTAMP", "ASSET","VWAP"]]
    print(tbIns)
    print("===========================================================================================")
    print("===========================================================================================")
    tbIns['BARTIMESTAMP'] = pd.to_datetime(tbIns['BARTIMESTAMP'],  errors='coerce', utc=True)
    tbIns.set_index("BARTIMESTAMP"   , inplace=True)
    tbIns.sort_index(ascending = True, inplace=True)

    # insert that first set of rows
    con=dbConnect()
    cur = con.cursor()
    lines=[]
    for index,row in tbIns.iterrows():
        try:
            line=[0,1,2,3,4,5,6,7,8]
            line[0]=row["TICKER"]
            #line[0]="SSS"
            line[1]=row['OPEN']
            line[2]=row['HIGH']
            line[3]=row['LOW']
            line[4]=row['CLOSE']
            line[5]=row['VOLUME']
            line[6]=index.to_pydatetime()
            line[7]=1
            line[8]=row['VWAP']
            #print(index.to_pydatetime())
            lines.append(line)

            #print(lines)
            print(line)
            cur.execute("insert into FILL_OHLCV(TICKER,OPEN,HIGH,LOW,CLOSE,VOLUME,BARTIMESTAMP,ASSET,VWAP) values (:TICKER, :OPEN,:HIGH,:LOW,:CLOSE,:VOLUME,:BARTIMESTAMP,:1,:2)",line)
            con.commit()
        except Exception as e:
            #cur.execute("insert into FILL_OHLCV(TICKER,OPEN,HIGH,LOW,CLOSE,VOLUME,BARTIMESTAMP,ASSET,VWAP) values (:TICKER, :OPEN,:HIGH,:LOW,:CLOSE,:VOLUME,:BARTIMESTAMP,:1,:2)",line)
            con.commit()
            print("4444")
            print(str(e))
            print(line)
    print(f"Insert Data Of EWI Last of {file} Done")
    print("===========================================================================================")
    print("===========================================================================================")