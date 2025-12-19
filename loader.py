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
            DATE TEXT PRIMARY KEY NOT NULL,
            MAXTP DOUBLE,
            MINTP DOUBLE,
            MEANTP DOUBLE,
            DIFFTP DOUBLE,
            GMIN DOUBLE,
            SOIL DOUBLE
        );""")
    else:
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {file}(
            DATE TEXT PRIMARY KEY NOT NULL,
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
    lastDate = ""
    try:
        for row in csvFile.itertuples():
            day = row.date[0:2]
            month = convertMonth(row.date[3:6])
            year = convertYear(row.date[7:9])
            dateIn = f"{year}-{month}-{day}"

            # Ensure no duplicate records are read
            if(dateIn!=lastDate):
                # Check if temp values are " ", if so set to 0, otherwise pass in value from csv
                if(hasattr(row, "maxt")):
                    if(row.maxt==" "):
                        maxt = None
                    else:
                        maxt=float(row.maxt)
                    if(row.mint==" "):
                        mint = None
                    else:
                        mint=float(row.mint)
                else:
                    if(row.maxtp==" "):
                        maxt = None
                    else:
                        maxt=float(row.maxtp)
                    if(row.mintp==" "):
                        mint = None
                    else:
                        mint=float(row.mintp)
                if(row.gmin==" "):
                    gmin = None
                else:
                    gmin = float(row.gmin)
                if(row.soil==" "):
                    soil = None
                else:
                    soil = float(row.soil)
                
                if(hasattr(row, "maxtp")):
                    insertSQL = f"""INSERT INTO {file} (DATE,MAXTP,MINTP,MEANTP,DIFFTP,GMIN,SOIL) VALUES (?,?,?,?,?,?,?);"""
                else:
                    insertSQL = f"""INSERT INTO {file} (DATE,MAXT,MINT,MEANT,DIFFT,GMIN,SOIL) VALUES (?,?,?,?,?,?,?);"""

                if(mint!=None and maxt!=None):
                    args = (dateIn, maxt, mint, round((maxt+mint)/2, 2), round(maxt-mint, 2), gmin, soil)
                    cursor.execute(insertSQL, args)
                elif((mint==None) != (maxt==None)):    
                    args = (dateIn, maxt, mint, None, None, gmin, soil)
                    cursor.execute(insertSQL, args)
                                
                lastDate=dateIn

    except Exception as e:
        print("An error has occured while writing dataframe to table: "+str(e))
    db.commit() # Commit changes
print("Finished loading data to database")