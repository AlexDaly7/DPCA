import pandas as pd
import sqlite3 as sqlite
import datetime
import sys

tableList = ["airport", "casement", "dunlaoghaire", "glasnevin", "merrion", "phoenixpark"]
fieldList = ["MAXTP", "MINTP", "MAXT", "MINT", "GMIN", "SOIL"]
db = sqlite.connect("database.db")
cur = db.cursor()

mainFrame = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")

# Create table
#createSQL = f"""CREATE TABLE IF NOT EXISTS dublin(
#            DATE TEXT PRIMARY KEY NOT NULL,
#            MAXTP DOUBLE,
#            MINTP DOUBLE,
#            MEANTP DOUBLE,
#            DIFFTP DOUBLE,
#            MAXT DOUBLE,
#            MINT DOUBLE,
#            MEANT DOUBLE,
#            DIFFT DOUBLE,
#            GMIN DOUBLE,
#            SOIL DOUBLE
#        );"""
#cur.execute(createSQL)

# Create dataframes to columns for aggregation
maxtp = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")
mintp = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")
maxt = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")
mint = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")
gmin = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")
soil = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")
dublinAvg = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")
#dublinAvg["DATE"] = dublinAvg.index

def getIndex(date):
    return mainFrame.index.get_loc(mainFrame[mainFrame['DATE'] == date].index[0])
    index = (mainFrame.index)
    return index.astype(int)

for table in tableList:
    tablePD = pd.read_sql_query(f"SELECT * FROM {table};", db, "DATE")
    if("MAXTP" in tablePD.columns):
        maxtp = pd.concat([maxtp, tablePD["MAXTP"]], axis=1)
        mintp = pd.concat([mintp, tablePD["MINTP"]], axis=1)
    else:
        maxt = pd.concat([maxt, tablePD["MAXT"]], axis=1)
        mint = pd.concat([mint, tablePD["MINT"]], axis=1)
    gmin = pd.concat([gmin, tablePD["GMIN"]], axis=1)
    soil = pd.concat([soil, tablePD["SOIL"]], axis=1)

#def getAvg(field, table, ):
casement = pd.read_sql_query("SELECT * FROM casement", db, "DATE")
print(mint)

maxtp = maxtp.agg("max", axis="columns").round(2)
mintp = mintp.agg("min", axis="columns").round(2)
maxt = maxt.agg("max", axis="columns").round(2)
mint = mint.agg("min", axis="columns").round(2)
gmin = gmin.agg("mean", axis="columns").round(2)
soil = soil.agg("mean", axis="columns").round(2)

# Calculate mean for temp and air temp
meantp = pd.concat([maxtp, mintp], axis=1)
meantp = meantp.agg("mean", axis="columns").round(2)

meant = pd.concat([maxt, mint], axis=1)
meant = meant.agg("mean", axis="columns").round(2)

dublinAvg = pd.concat([dublinAvg, maxtp], axis=1)
dublinAvg = pd.concat([dublinAvg, mintp], axis=1)
dublinAvg = pd.concat([dublinAvg, meantp], axis=1)
dublinAvg["DIFFTP"] = maxtp - mintp
dublinAvg = pd.concat([dublinAvg, maxt], axis=1)
dublinAvg = pd.concat([dublinAvg, mint], axis=1)
dublinAvg = pd.concat([dublinAvg, meant], axis=1)
dublinAvg["DIFFT"] = maxt - mint
dublinAvg = pd.concat([dublinAvg, gmin], axis=1)
dublinAvg = pd.concat([dublinAvg, soil], axis=1)
dublinAvg.reset_index(drop=True)
dublinAvg.columns=["MAXTP","MINTP","MEANTP","DIFFTP","MAXT","MINT","MEANT","DIFFT","GMIN","SOIL"]
print(dublinAvg)

dublinAvg.to_sql("dublin", db)
#for row in dublinAvg.itertuples():

#print(pd.concat([mainFrame, casement["MINTP"]], axis=1))