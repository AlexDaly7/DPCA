import sqlite3 as sqlite
import pandas as pd
import matplotlib.pyplot as plt

db = sqlite.connect("database.db")

table = pd.read_sql_query("SELECT DIFFTP, DATE FROM dublin", db)
table["DATE"] = pd.to_datetime(table["DATE"]) # Convert to datetime objects
table = table.set_index(table["DATE"]) # Set to index

print(table)

plt.title("Difference between min and mix air temperature")
plt.xlabel("Date")
plt.ylabel("Difference between minimum and maximum air temperature (Degrees Celcius)")

plt.plot(table["DIFFTP"]) # Plot column
plt.grid()

plt.show()