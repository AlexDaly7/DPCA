import sqlite3 as sqlite
import pandas as pd
import matplotlib.pyplot as plt

db = sqlite.connect("database.db")

table = pd.read_sql_query("SELECT SOIL, MAXTP, MINTP, DATE FROM dublin", db)
table["DATE"] = pd.to_datetime(table["DATE"]) # Convert to datetime objects
table = table.set_index(table["DATE"]) # Set to index

plt.title("Maximum air temperature and mean 10cm soil temperature")
plt.xlabel("Date")
plt.ylabel("Degrees (Celcius)")

plt.plot(table["MAXTP"]) # Plot columns
plt.plot(table["SOIL"])
plt.grid()

plt.show()

print(table.corr(method="pearson"))