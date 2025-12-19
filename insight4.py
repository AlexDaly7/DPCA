import sqlite3 as sqlite
import pandas as pd
import matplotlib.pyplot as plt

db = sqlite.connect("database.db")

table = pd.read_sql_query("SELECT * FROM dublin", db) 
table["DATE"] = pd.to_datetime(table["DATE"]) # Convert to datetime object
table = table.set_index(table["DATE"]) # Set to index

plt.title("Maxmimum, mean and minimum temperature")
plt.xlabel("Date")
plt.ylabel("Degrees (Celcius)")

plt.plot(table["MAXT"]) # Plot columns
plt.plot(table["MINT"])
plt.plot(table["MEANT"])
plt.grid()

plt.show()