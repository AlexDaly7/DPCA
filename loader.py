import pandas as pd
import sqlite3 as sqllite
import datetime
import sys

csvList = ["airport", "casement", "dunlaoghaire", "glasnevin", "merrion", "phoenixpark"]

# Convert dates to better format
def convertMonth(month):
    return datetime.datetime.strptime(month, '%b').strftime('%m')
    #Function gotten from https://stackoverflow.com/a/45605037, which is sourced from python docs
def convertYear(year):
    year=int(year)
    if(year>=40):
        year=year+1900
    else:
        year=year+2000
    return year

db = sqllite.connect("database.db")
cursor = db.cursor()
path = "./csvfiles/"

for file in csvList:
    # Load csv file
    url = path+file+".csv"
    try:
        csvFile = pd.read_csv(url)
    except Exception as e:
        print("A problem has occured while loading csv file: "+str(e))
        sys.exit(1)


    # Create table if doesnt exist
    if(hasattr(csvFile, "maxtp")):
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {file}(
            DATE DATE PRIMARY KEY NOT NULL,
            MAXTP DOUBLE,
            MINTP DOUBLE,
            MEANTP DOUBLE,
            DIFFTP DOUBLE,
            GMIN DOUBLE,
            SOIL DOUBLE
        );""")
    else:
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {file}(
            DATE DATE PRIMARY KEY NOT NULL,
            MAXT DOUBLE,
            MINT DOUBLE,
            MEANT DOUBLE,
            DIFFT DOUBLE,
            GMIN DOUBLE,
            SOIL DOUBLE
        );""")
    # MAXTP is the max air temp
    # MINTP is the min air temp
    # MEANTP is the mean air temp
    # DIFFTP is the different between max and min air temp
    # GMIN is the 09UTC grass min temp
    # SOIL is the mean 10cm soil temp

    # Iterate on each row in loaded csv dataframe
    print(f"Iterating on {file}")
    try:
        for row in csvFile.itertuples():
            day = row.date[0:2]
            month = convertMonth(row.date[3:6])
            year = convertYear(row.date[7:9])
            dateIn = datetime.date(int(year), int(month), int(day))

            # Check if temp values are " ", if so set to 0, otherwise pass in value from csv
            if(hasattr(row, "maxt")):
                if(row.maxt==" "):
                    maxt = 0
                else:
                    maxt=float(row.maxt)
                if(row.mint==" "):
                    mint = 0
                else:
                    mint=float(row.mint)
            else:
                if(row.maxtp==" "):
                    maxt = 0
                else:
                    maxt=float(row.maxtp)
                if(row.mintp==" "):
                    mint = 0
                else:
                    mint=float(row.mintp)
            if(mint!=0 or maxt!=0):
                if(hasattr(row, "maxtp")):
                    insertSQL = f"""INSERT INTO {file} (DATE,MAXTP,MINTP,MEANTP,DIFFTP,GMIN,SOIL) VALUES (?,?,?,?,?,?,?);"""
                else:
                    insertSQL = f"""INSERT INTO {file} (DATE,MAXT,MINT,MEANT,DIFFT,GMIN,SOIL) VALUES (?,?,?,?,?,?,?);"""
                args = (dateIn, maxt, mint, (maxt+mint)/2, maxt-mint, row.gmin, row.soil)
                cursor.execute(insertSQL, args)
    except Exception as e:
        print("An error has occured while writing dataframe to table: "+str(e))
    db.commit() # Commit changes