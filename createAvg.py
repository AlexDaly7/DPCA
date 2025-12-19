import pandas as pd
import sqlite3 as sqlite

tableList = ["airport", "casement", "dunlaoghaire", "glasnevin", "merrion", "phoenixpark"]
db = sqlite.connect("database.db")
cur = db.cursor()

mainFrame = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")

# Create dataframes to columns for aggregation
maxtp = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")
mintp = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")
maxt = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")
mint = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")
gmin = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")
soil = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")
dublinAvg = pd.read_sql_query("SELECT DATE FROM airport;", db, "DATE")

for table in tableList: # For each table in db
    tablePD = pd.read_sql_query(f"SELECT * FROM {table};", db, "DATE")
    if("MAXTP" in tablePD.columns):
        maxtp = pd.concat([maxtp, tablePD["MAXTP"]], axis=1)
        mintp = pd.concat([mintp, tablePD["MINTP"]], axis=1)
    else:
        maxt = pd.concat([maxt, tablePD["MAXT"]], axis=1)
        mint = pd.concat([mint, tablePD["MINT"]], axis=1)
    gmin = pd.concat([gmin, tablePD["GMIN"]], axis=1)
    soil = pd.concat([soil, tablePD["SOIL"]], axis=1)

casement = pd.read_sql_query("SELECT * FROM casement", db, "DATE")

# Aggregate fields into one column for Dublin table
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

# Add columns into Dublin DataFrame
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
# Name columns
dublinAvg.columns=["MAXTP","MINTP","MEANTP","DIFFTP","MAXT","MINT","MEANT","DIFFT","GMIN","SOIL"]

dublinAvg.to_sql("dublin", db) # DataFrame to database